package queue

import (
	"container/list"
	"errors"
	"log"
	"sync"
	"time"
)

type line struct {
	mu       sync.Mutex
	name     string
	head     uint64
	recycle  time.Duration
	inflight *list.List
	t        *topic
}

type lineStore struct {
	Head      uint64
	Recycle   time.Duration
	Inflights []inflightMessage
}

func NewLine(name string, recycle time.Duration) (*line, error) {
	inflight := list.New()
	l := new(line)
	l.name = name
	l.head = 0
	l.recycle = recycle
	l.inflight = inflight

	return l, nil
}

func loadLine(name string, lineStoreValue lineStore) (*line, error) {
	log.Printf("loading inflights: %v", lineStoreValue.Inflights)
	inflight := list.New()
	for index, _ := range lineStoreValue.Inflights {
		inflight.PushBack(&lineStoreValue.Inflights[index])
	}

	l := new(line)
	l.name = name
	l.head = lineStoreValue.Head
	l.recycle = lineStoreValue.Recycle
	l.inflight = inflight

	return l, nil
}

func (l *line) exportLine() (*lineStore, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	inflights := make([]inflightMessage, l.inflight.Len())
	i := 0
	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		inflights[i] = *msg
		i++
	}
	log.Printf("inflights: %v", inflights)

	ls := new(lineStore)
	ls.Head = l.head
	ls.Recycle = l.recycle
	ls.Inflights = inflights
	return ls, nil
}

func (l *line) Pop() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.recycle > 0 {
		for m := l.inflight.Front(); m != nil; m = m.Next() {
			msg := m.Value.(*inflightMessage)
			log.Printf("finding key[%s/%d] in flights...", l.name, msg.Tid)
			if time.Now().Before(msg.Exptime) {
				log.Printf("key[%s/%d] is not expired, continue...", l.name, msg.Tid)
				continue
			} else {
				tid := msg.Tid
				msg.Exptime = time.Now().Add(l.recycle)
				log.Printf("key[%s/%d] is expired.", l.name, tid)

				log.Printf("key[%s/%d] poped.", l.name, tid)
				return l.t.getData(tid)
			}
		}
	}

	if l.head >= l.t.tail {
		log.Printf("line[%s] is blank. head:%d - tail:%d", l.name, l.head, l.t.tail)
		return nil, errors.New(ErrNone)
	}

	data, err := l.t.getData(l.head)
	if err != nil {
		return nil, err
	}
	log.Printf("key[%s/%d] poped.", l.name, l.head)

	if l.recycle > 0 {
		msg := new(inflightMessage)
		msg.Tid = l.head
		msg.Exptime = time.Now().Add(l.recycle)

		l.inflight.PushBack(msg)
		log.Printf("key[%s/%d] flighted.", l.name, l.head)
	}
	l.head++

	return data, nil
}
