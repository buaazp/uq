package queue

import (
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
	tail  uint64
	q     *UnitedQueue
}

type topicStore struct {
	Lines []string
	Tail  uint64
}

func NewTopic(name string) (*topic, error) {
	lines := make(map[string]*line)
	t := new(topic)
	t.name = name
	t.lines = lines
	t.tail = 0

	return t, nil
}

func (t *topic) exportTopic() (*topicStore, error) {
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

	l, err := NewLine(name, recycle)
	if err != nil {
		return err
	}

	t.lines[name] = l
	l.t = t

	log.Printf("line[%s:%v] created.", name, recycle)
	return nil
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

func (t *topic) pop(name string) ([]byte, error) {
	l, ok := t.lines[name]
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return nil, errors.New(ErrLineNotExisted)
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
