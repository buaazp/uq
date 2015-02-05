package queue

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"log"
	"sync"
	"time"
)

type line struct {
	name         string
	head         uint64
	headLock     sync.RWMutex
	recycle      time.Duration
	inflight     *list.List
	inflightLock sync.RWMutex
	t            *topic
}

type lineStore struct {
	Head      uint64
	Recycle   time.Duration
	Inflights []inflightMessage
}

func (l *line) pop() (uint64, []byte, error) {
	l.inflightLock.Lock()
	defer l.inflightLock.Unlock()

	if l.recycle > 0 {
		now := time.Now()

		m := l.inflight.Front()
		if m != nil {
			msg := m.Value.(*inflightMessage)
			if now.After(msg.Exptime) {
				// log.Printf("key[%s/%d] is expired.", l.name, msg.Tid)
				msg.Exptime = now.Add(l.recycle)
				data, err := l.t.getData(msg.Tid)
				if err != nil {
					return 0, nil, err
				}
				l.inflight.Remove(m)
				l.inflight.PushBack(msg)
				// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, msg.Tid)
				return msg.Tid, data, nil
			}
		}
	}

	l.headLock.Lock()
	defer l.headLock.Unlock()

	topicTail := l.t.getTail()
	if l.head >= topicTail {
		// log.Printf("line[%s] is blank. head:%d - tail:%d", l.name, l.head, l.t.tail)
		return 0, nil, errors.New(ErrNone)
	}

	data, err := l.t.getData(l.head)
	if err != nil {
		return 0, nil, err
	}
	// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, l.head)

	if l.recycle > 0 {
		msg := new(inflightMessage)
		msg.Tid = l.head
		msg.Exptime = time.Now().Add(l.recycle)

		l.inflight.PushBack(msg)
		// log.Printf("key[%s/%s/%d] flighted.", l.t.name, l.name, l.head)
	}
	tid := l.head
	l.head++

	return tid, data, nil
}

func (l *line) confirm(id uint64) error {
	if l.recycle == 0 {
		return nil
	}

	l.inflightLock.Lock()
	defer l.inflightLock.Unlock()

	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		if msg.Tid < id {
			continue
		} else if msg.Tid == id {
			l.inflight.Remove(m)
			log.Printf("key[%s/%s/%d] comfirmed.", l.t.name, l.name, id)
			return nil
		} else {
			break
		}
	}

	return errors.New(ErrNotDelivered)
}

func (l *line) exportLine() error {
	log.Printf("start export line[%s]...", l.name)

	lineStoreValue, err := l.genLineStore()
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(lineStoreValue)
	if err != nil {
		return err
	}

	lineStoreKey := l.t.name + "/" + l.name
	err = l.t.q.storage.Set(lineStoreKey, buffer.Bytes())
	if err != nil {
		return err
	}

	log.Printf("line[%s] export finisded.", l.name)
	return nil
}

func (l *line) genLineStore() (*lineStore, error) {
	l.inflightLock.RLock()
	defer l.inflightLock.RUnlock()

	inflights := make([]inflightMessage, l.inflight.Len())
	i := 0
	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		inflights[i] = *msg
		i++
	}
	// log.Printf("inflights: %v", inflights)

	ls := new(lineStore)
	l.headLock.RLock()
	ls.Head = l.head
	l.headLock.RUnlock()
	ls.Recycle = l.recycle
	ls.Inflights = inflights
	return ls, nil
}

func (l *line) isBlank() bool {
	l.inflightLock.RLock()
	defer l.inflightLock.RUnlock()
	if l.recycle > 0 && l.inflight.Len() > 0 {
		log.Printf("%s l.recycle: %d l.inflight.Len(): %d", l.name, l.recycle, l.inflight.Len())
		return false
	}

	topicTail := l.t.getTail()
	l.headLock.RLock()
	defer l.headLock.RUnlock()
	if l.head < topicTail {
		log.Printf("%s l.head: %d %s t.tail: %d", l.name, l.head, l.t.name, topicTail)
		return false
	}

	return true
}
