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
	// log.Printf("loading inflights: %v", lineStoreValue.Inflights)
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

	log.Printf("start export line...")
	inflights := make([]inflightMessage, l.inflight.Len())
	i := 0
	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		inflights[i] = *msg
		i++
	}
	// log.Printf("inflights: %v", inflights)

	ls := new(lineStore)
	ls.Head = l.head
	ls.Recycle = l.recycle
	ls.Inflights = inflights
	return ls, nil
}

func (l *line) pop() ([]byte, error) {
	if l.recycle > 0 {
		now := time.Now()
		// log.Printf("inflight len: %d", l.inflight.Len())
		for m := l.inflight.Front(); m != nil; m = m.Next() {
			msg := m.Value.(*inflightMessage)
			// log.Printf("finding key[%s/%d] in flights...", l.name, msg.Tid)
			if now.Before(msg.Exptime) {
				// log.Printf("key[%s/%d] is not expired, continue...", l.name, msg.Tid)
				continue
			} else {
				l.mu.Lock()
				defer l.mu.Unlock()

				msg.Exptime = time.Now().Add(l.recycle)
				// log.Printf("key[%s/%d] is expired.", l.name, msg.Tid)

				// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, msg.Tid)
				return l.t.getData(msg.Tid)
			}
		}
	}

	if l.head >= l.t.tail {
		// log.Printf("line[%s] is blank. head:%d - tail:%d", l.name, l.head, l.t.tail)
		return nil, errors.New(ErrNone)
	}

	data, err := l.t.getData(l.head)
	if err != nil {
		return nil, err
	}
	// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, l.head)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.recycle > 0 {
		msg := new(inflightMessage)
		msg.Tid = l.head
		msg.Exptime = time.Now().Add(l.recycle)

		l.inflight.PushBack(msg)
		// log.Printf("key[%s/%s/%d] flighted.", l.t.name, l.name, l.head)
	}
	l.head++

	return data, nil
}

func (l *line) confirm(id uint64) error {
	if l.recycle == 0 {
		return nil
	}

	confirmed := false
	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		if msg.Tid < id {
			continue
		} else if msg.Tid == id {
			l.mu.Lock()
			defer l.mu.Unlock()

			l.inflight.Remove(m)
			confirmed = true
			break
		} else {
			break
		}
	}

	if confirmed {
		log.Printf("key[%s/%s/%d] comfirmed.", l.t.name, l.name, id)
		return nil
	}
	return errors.New(ErrNotDelivered)
}
