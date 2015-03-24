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
	ihead        uint64
	imap         map[uint64]bool
	t            *topic
}

type lineStore struct {
	Head      uint64
	Recycle   time.Duration
	Inflights []inflightMessage
	Ihead     uint64
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
	tid := l.head

	topicTail := l.t.getTail()
	if l.head >= topicTail {
		// log.Printf("line[%s] is blank. head:%d - tail:%d", l.name, l.head, l.t.tail)
		return 0, nil, errors.New(ErrNone)
	}

	data, err := l.t.getData(tid)
	if err != nil {
		return 0, nil, err
	}
	// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, l.head)

	if l.recycle > 0 {
		msg := new(inflightMessage)
		msg.Tid = tid
		msg.Exptime = time.Now().Add(l.recycle)

		l.inflight.PushBack(msg)
		// log.Printf("key[%s/%s/%d] flighted.", l.t.name, l.name, l.head)
		l.imap[tid] = true
	}
	l.head++

	return tid, data, nil
}

func (l *line) mPop(n int) ([]uint64, [][]byte, error) {
	l.inflightLock.Lock()
	defer l.inflightLock.Unlock()

	fc := 0
	ids := make([]uint64, 0)
	datas := make([][]byte, 0)
	if l.recycle > 0 {
		now := time.Now()
		for m := l.inflight.Front(); m != nil && fc < n; m = m.Next() {
			msg := m.Value.(*inflightMessage)
			if now.After(msg.Exptime) {
				msg := m.Value.(*inflightMessage)
				data, err := l.t.getData(msg.Tid)
				if err != nil {
					return nil, nil, err
				}
				ids = append(ids, msg.Tid)
				datas = append(datas, data)
				fc++
			} else {
				break
			}
		}
		exptime := now.Add(l.recycle)
		for i := 0; i < fc; i++ {
			m := l.inflight.Front()
			msg := m.Value.(*inflightMessage)
			msg.Exptime = exptime
			l.inflight.Remove(m)
			l.inflight.PushBack(msg)
		}
		if fc >= n {
			return ids, datas, nil
		}
	}

	l.headLock.Lock()
	defer l.headLock.Unlock()

	for ; fc < n; fc++ {
		topicTail := l.t.getTail()
		if l.head >= topicTail {
			// log.Printf("line[%s] is blank. head:%d - tail:%d", l.name, l.head, l.t.tail)
			break
		}

		data, err := l.t.getData(l.head)
		if err != nil {
			return nil, nil, err
		}
		// log.Printf("key[%s/%s/%d] poped.", l.t.name, l.name, l.head)
		ids = append(ids, l.head)
		datas = append(datas, data)

		if l.recycle > 0 {
			msg := new(inflightMessage)
			msg.Tid = l.head
			msg.Exptime = time.Now().Add(l.recycle)

			l.inflight.PushBack(msg)
			// log.Printf("key[%s/%s/%d] flighted.", l.t.name, l.name, l.head)
		}
		l.head++
	}

	if len(ids) > 0 {
		return ids, datas, nil
	}
	return nil, nil, errors.New(ErrNone)
}

func (l *line) confirm(id uint64) error {
	if l.recycle == 0 {
		return errors.New(ErrNotDelivered)
	}

	l.headLock.RLock()
	defer l.headLock.RUnlock()
	head := l.head
	if id >= head {
		return errors.New(ErrNotDelivered)
	}

	l.inflightLock.Lock()
	defer l.inflightLock.Unlock()

	for m := l.inflight.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*inflightMessage)
		if msg.Tid == id {
			l.inflight.Remove(m)
			log.Printf("key[%s/%s/%d] comfirmed.", l.t.name, l.name, id)
			l.imap[id] = false
			l.updateiHead()
			return nil
		}
	}

	return errors.New(ErrNotDelivered)
}

func (l *line) updateiHead() {
	for l.ihead < l.head {
		id := l.ihead
		fl, ok := l.imap[id]
		if !ok {
			l.ihead++
			continue
		}
		if fl {
			return
		} else {
			delete(l.imap, id)
			l.ihead++
		}
	}
}

func (l *line) mConfirm(ids []uint64) (int, error) {
	if l.recycle == 0 {
		return 0, errors.New(ErrNotDelivered)
	}

	l.headLock.RLock()
	head := l.head
	l.headLock.RUnlock()

	l.inflightLock.Lock()
	defer l.inflightLock.Unlock()

	var confirmed int = 0
	for _, id := range ids {
		if id >= head {
			log.Printf("ID[%d] is Not Delivered", id)
			continue
		}

		for m := l.inflight.Front(); m != nil; m = m.Next() {
			msg := m.Value.(*inflightMessage)
			if msg.Tid == id {
				l.inflight.Remove(m)
				log.Printf("key[%s/%s/%d] comfirmed.", l.t.name, l.name, id)
				confirmed++
			}
		}
	}

	if confirmed == 0 {
		return 0, errors.New(ErrNotDelivered)
	}
	return confirmed, nil
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
	ls.Ihead = l.ihead
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
