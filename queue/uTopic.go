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

func (t *topic) exportStore() (*topicStore, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

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

func (t *topic) Push(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s:%d", t.name, t.tail)
	err := t.q.setData(key, data)
	if err != nil {
		return err
	}
	t.tail++
	return nil
}

func (t *topic) Pop(name string) ([]byte, error) {
	l, ok := t.lines[name]
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return nil, errors.New(ErrLineNotExisted)
	}

	return l.Pop()
}

func (t *topic) getData(id uint64) ([]byte, error) {
	key := fmt.Sprintf("%s:%d", t.name, id)
	return t.q.getData(key)
}
