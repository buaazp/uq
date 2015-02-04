package queue

import (
	"bytes"
	"container/list"
	"encoding/gob"
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
	log.Printf("topic[%s] is starting...", t.name)
	go t.backgroundClean()
	go t.backgroundBackup()
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

	err = t.exportTopic()
	if err != nil {
		delete(t.lines, name)
		return err
	}

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

func (t *topic) exportTopic() error {
	topicStoreValue, err := t.genTopicStore()
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(topicStoreValue)
	if err != nil {
		return err
	}

	err = t.q.storage.Set(t.name, buffer.Bytes())
	if err != nil {
		return err
	}

	// log.Printf("topic[%s] export finisded.", t.name)
	return nil
}

func (t *topic) genTopicStore() (*topicStore, error) {
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

func (t *topic) Push(data []byte) error {
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

	return l.Pop()
}

func (t *topic) confirm(name string, id uint64) error {
	l, ok := t.lines[name]
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return errors.New(ErrLineNotExisted)
	}

	return l.Confirm(id)
}

func (t *topic) getData(id uint64) ([]byte, error) {
	key := fmt.Sprintf("%s:%d", t.name, id)
	return t.q.getData(key)
}

func (t *topic) backgroundClean() {
	defer func() {
		log.Printf("background clean exit.")
	}()

	cleanTick := time.NewTicker(BgCleanInterval)
	for {
		select {
		case <-cleanTick.C:
			quit := t.Clean()
			if quit {
				log.Printf("t.clean return quit: %v", quit)
				return
			}
		case <-t.quit:
			log.Printf("background clean catched quit")
			return
		}
	}
}

func (t *topic) Clean() (quit bool) {
	quit = false
	if t.isClear() {
		log.Printf("topic[%s] is clear. needn't clean.", t.name)
		return
	}

	if !t.isBlank() {
		log.Printf("topic[%s] is not blank. ignore clean.", t.name)
		return
	}

	topicTail := t.tail
	starting := t.head
	terminal := starting
	endTime := time.Now().Add(BgCleanTimeout)
	log.Printf("topic[%s] begin to clean at %d", t.name, starting)

	defer func() {
		if terminal != starting {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.head = terminal
			log.Printf("garbage[%d - %d] are cleaned", starting, terminal)
		}
	}()

	for i := starting; i < topicTail; i++ {
		select {
		case <-t.quit:
			log.Printf("catched quit at %d", terminal)
			return
		default:
			// nothing todo
		}

		if time.Now().After(endTime) {
			log.Printf("cleaning timeout, break at %d", terminal)
			return
		}

		key := fmt.Sprintf("%s:%d", t.name, i)
		err := t.q.delData(key)
		if err != nil {
			log.Printf("del data[%s] error; %s", key, err)
			return
		}
		terminal = i
	}

	return
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

func (t *topic) backgroundBackup() {
	defer func() {
		log.Printf("background backup exit.")
	}()

	backupTick := time.NewTicker(BgBackupInterval)
	for {
		select {
		case <-backupTick.C:
			t.Backup()
		case <-t.quit:
			log.Printf("background backup catched quit")
			return
		}
	}
}

func (t *topic) Backup() {
	log.Printf("topic[%s] is backing up...", t.name)
	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.exportTopic()
	if err != nil {
		log.Printf("export topic[%s] error: %s", t.name, err)
	}
	log.Printf("topic[%s] backup succ.", t.name)
}

func (t *topic) exportLines() error {
	for lineName, l := range t.lines {
		err := l.ExportLine()
		if err != nil {
			log.Printf("line[%s] export error: %s", lineName, err)
			continue
		}
	}

	log.Printf("topic[%s]'s all lines exported.", t.name)
	return nil
}
