package queue

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type topic struct {
	mu    sync.Mutex
	name  string
	lines map[string]*line
	head  uint64
	tail  uint64
	q     *UnitedQueue

	quit chan bool
}

type topicStore struct {
	Lines []string
	Head  uint64
	Tail  uint64
}

func (t *topic) start() {
	go t.backgroundClean()
}

func (t *topic) genTopicStore() (*topicStore, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log.Printf("start export topic...")
	lines := make([]string, len(t.lines))
	i := 0
	for _, line := range t.lines {
		lines[i] = line.name
		i++
	}

	ts := new(topicStore)
	ts.Lines = lines
	ts.Head = t.head
	ts.Tail = t.tail

	return ts, nil
}

func (t *topic) CreateLine(name string, recycle time.Duration) error {
	_, ok := t.lines[name]
	if ok {
		return errors.New(ErrLineExisted)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	l, err := t.newLine(name, recycle)
	if err != nil {
		return err
	}

	t.lines[name] = l

	log.Printf("line[%s:%v] created.", name, recycle)
	return nil
}

func (t *topic) newLine(name string, recycle time.Duration) (*line, error) {
	inflight := list.New()
	l := new(line)
	l.name = name
	l.head = t.head
	l.recycle = recycle
	l.inflight = inflight
	l.t = t

	return l, nil
}

func (t *topic) loadLine(lineName string, lineStoreValue lineStore) (*line, error) {
	// log.Printf("loading inflights: %v", lineStoreValue.Inflights)
	inflight := list.New()
	for index, _ := range lineStoreValue.Inflights {
		inflight.PushBack(&lineStoreValue.Inflights[index])
	}

	l := new(line)
	l.name = lineName
	l.head = lineStoreValue.Head
	l.recycle = lineStoreValue.Recycle
	l.inflight = inflight
	l.t = t

	return l, nil
}

func (t *topic) push(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s:%d", t.name, t.tail)
	err := t.q.setData(key, data)
	if err != nil {
		return err
	}
	// log.Printf("key[%s][%s] pushed.", key, string(data))

	t.tail++
	return nil
}

func (t *topic) pop(name string) (uint64, []byte, error) {
	l, ok := t.lines[name]
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return 0, nil, errors.New(ErrLineNotExisted)
	}

	return l.pop()
}

func (t *topic) confirm(name string, id uint64) error {
	l, ok := t.lines[name]
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return errors.New(ErrLineNotExisted)
	}

	return l.confirm(id)
}

func (t *topic) getData(id uint64) ([]byte, error) {
	key := fmt.Sprintf("%s:%d", t.name, id)
	return t.q.getData(key)
}

func (t *topic) backgroundClean() {
	defer func() {
		log.Printf("background clean exit.")
	}()

	tick := time.NewTicker(BgCleanInterval)
	for {
		select {
		case quit := <-t.quit:
			log.Printf("background catched quit: %v", quit)
			return
		case <-tick.C:
			quit := t.clean()
			if quit {
				log.Printf("t.clean quit: %v", quit)
				return
			}
		}
	}
}

func (t *topic) clean() bool {
	if t.isClear() {
		log.Printf("topic[%s] is clear. needn't clean.", t.name)
		return false
	}

	if !t.isBlank() {
		log.Printf("topic[%s] is not blank. ignore clean.", t.name)
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	endTime := time.Now().Add(BgCleanTimeout)
	log.Printf("topic[%s] begin to clean", t.name)
	for i := t.head; i < t.tail; i++ {
		select {
		case quit := <-t.quit:
			log.Printf("i: %d catched quit: %v", i, quit)
			return true
		default:
			// nothing todo
		}

		if time.Now().After(endTime) {
			log.Printf("i: %d cleaning timeout, break", i)
			return false
		}

		key := fmt.Sprintf("%s:%d", t.name, i)
		err := t.q.delData(key)
		if err != nil {
			log.Printf("del data error; %s", err)
			return false
		}
		t.head = i
		// log.Printf("garbage[%s] is cleaned", key)
	}
	log.Printf("garbage[%d - %d] are cleaned", t.head, t.tail)

	return false
}

func (t *topic) isClear() bool {
	if t.tail == 0 || t.head == t.tail-1 {
		log.Printf("%s t.head: %d t.tail: %d", t.name, t.head, t.tail)
		return true
	}
	log.Printf("topic[%s] is clear.", t.name)
	return false
}

func (t *topic) isBlank() bool {
	if len(t.lines) == 0 {
		log.Printf("topic[%s] has no line.", t.name)
		return false
	}

	for _, l := range t.lines {
		if l.recycle > 0 && l.inflight.Len() > 0 {
			log.Printf("%s l.recycle: %d l.inflight.Len(): %d", l.name, l.recycle, l.inflight.Len())
			return false
		}

		if l.head < t.tail {
			log.Printf("%s l.head: %d %s t.tail: %d", l.name, l.head, t.name, t.tail)
			return false
		}
	}

	log.Printf("topic[%s] is blank.", t.name)
	return true
}
