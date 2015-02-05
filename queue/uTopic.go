package queue

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/buaazp/uq/utils"
)

type topic struct {
	name      string
	lines     map[string]*line
	linesLock sync.RWMutex
	head      uint64
	headLock  sync.RWMutex
	tail      uint64
	tailLock  sync.RWMutex
	q         *UnitedQueue

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
	t.linesLock.RLock()
	_, ok := t.lines[name]
	t.linesLock.RUnlock()
	if ok {
		return errors.New(ErrLineExisted)
	}

	l, err := t.newLine(name, recycle)
	if err != nil {
		return err
	}

	t.linesLock.Lock()
	t.lines[name] = l
	t.linesLock.Unlock()

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
	t.linesLock.RLock()
	defer t.linesLock.RUnlock()

	lines := make([]string, len(t.lines))
	i := 0
	for _, line := range t.lines {
		lines[i] = line.name
		i++
	}

	ts := new(topicStore)
	ts.Lines = lines
	t.headLock.RLock()
	ts.Head = t.head
	t.headLock.RUnlock()
	t.tailLock.RLock()
	ts.Tail = t.tail
	t.tailLock.RUnlock()

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
	t.tailLock.Lock()
	defer t.tailLock.Unlock()

	key := utils.Acati(t.name, ":", t.tail)
	err := t.q.setData(key, data)
	if err != nil {
		return err
	}
	// log.Printf("key[%s][%s] pushed.", key, string(data))

	t.tail++

	return nil
}

func (t *topic) pop(name string) (uint64, []byte, error) {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return 0, nil, errors.New(ErrLineNotExisted)
	}

	return l.Pop()
}

func (t *topic) confirm(name string, id uint64) error {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return errors.New(ErrLineNotExisted)
	}

	return l.Confirm(id)
}

func (t *topic) getData(id uint64) ([]byte, error) {
	key := utils.Acati(t.name, ":", id)
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

	t.tailLock.RLock()
	topicTail := t.tail
	t.tailLock.RUnlock()
	t.headLock.RLock()
	starting := t.head
	t.headLock.RUnlock()
	terminal := starting
	endTime := time.Now().Add(BgCleanTimeout)
	log.Printf("topic[%s] begin to clean at %d", t.name, starting)

	defer func() {
		if terminal != starting {
			t.headLock.Lock()
			t.head = terminal
			t.headLock.Unlock()
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

		key := utils.Acati(t.name, ":", i)
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
	t.tailLock.RLock()
	topicTail := t.tail
	t.tailLock.RUnlock()
	t.headLock.RLock()
	topicHead := t.head
	t.headLock.RUnlock()
	if topicTail == 0 || topicHead == topicTail-1 {
		log.Printf("%s t.head: %d t.tail: %d", t.name, topicHead, topicTail)
		return true
	}
	log.Printf("topic[%s] is clear.", t.name)
	return false
}

func (t *topic) isBlank() bool {
	t.linesLock.RLock()
	defer t.linesLock.RUnlock()

	linesLen := len(t.lines)
	if linesLen == 0 {
		log.Printf("topic[%s] has no line.", t.name)
		return false
	}

	for _, l := range t.lines {
		if !l.isBlank() {
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

	err := t.exportTopic()
	if err != nil {
		log.Printf("export topic[%s] error: %s", t.name, err)
	}
	log.Printf("topic[%s] backup succ.", t.name)
}

func (t *topic) exportLines() error {
	t.linesLock.RLock()
	defer t.linesLock.RUnlock()

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

func (t *topic) getTail() uint64 {
	t.tailLock.RLock()
	defer t.tailLock.RUnlock()
	return t.tail
}
