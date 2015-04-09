package queue

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"log"
	"sync"
	"time"

	. "github.com/buaazp/uq/utils"
)

func init() {
	gob.Register(&topicStore{})
}

type topic struct {
	name      string
	lines     map[string]*line
	linesLock sync.RWMutex
	head      uint64
	headLock  sync.RWMutex
	headKey   string
	tail      uint64
	tailLock  sync.RWMutex
	tailKey   string
	q         *UnitedQueue

	quit chan bool
	wg   sync.WaitGroup
}

type topicStore struct {
	Lines []string
}

func (t *topic) start() {
	log.Printf("topic[%s] is starting...", t.name)
	go t.backgroundClean()
}

func (t *topic) createLine(name string, recycle time.Duration, fromEtcd bool) error {
	t.linesLock.RLock()
	_, ok := t.lines[name]
	t.linesLock.RUnlock()
	if ok {
		return NewError(
			ErrLineExisted,
			`topic createLine`,
		)
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

	if !fromEtcd {
		err := t.q.registerLine(t.name, l.name, l.recycle.String())
		if err != nil {
			log.Printf("register line error: %s", err)
		}
	}
	log.Printf("line[%s:%v] created.", name, recycle)
	return nil
}

func (t *topic) newLine(name string, recycle time.Duration) (*line, error) {
	inflight := list.New()
	imap := make(map[uint64]bool)
	l := new(line)
	l.name = name
	l.head = t.head
	l.headKey = t.name + "/" + name + KeyLineHead
	l.recycle = recycle
	l.recycleKey = t.name + "/" + name + KeyLineRecycle
	l.inflight = inflight
	l.ihead = t.head
	l.imap = imap
	l.t = t

	err := l.exportHead()
	if err != nil {
		return nil, err
	}
	err = l.exportRecycle()
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (t *topic) getHead() uint64 {
	t.headLock.RLock()
	defer t.headLock.RUnlock()
	return t.head
}

func (t *topic) getTail() uint64 {
	t.tailLock.RLock()
	defer t.tailLock.RUnlock()
	return t.tail
}

func (t *topic) exportHead() error {
	topicHeadData := make([]byte, 8)
	binary.LittleEndian.PutUint64(topicHeadData, t.head)
	err := t.q.storage.Set(t.headKey, topicHeadData)
	if err != nil {
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	return nil
}

func (t *topic) exportTail() error {
	topicTailData := make([]byte, 8)
	binary.LittleEndian.PutUint64(topicTailData, t.tail)
	err := t.q.storage.Set(t.tailKey, topicTailData)
	if err != nil {
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	return nil
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
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}

	err = t.q.storage.Set(t.name, buffer.Bytes())
	if err != nil {
		return NewError(
			ErrInternalError,
			err.Error(),
		)
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

	return ts, nil
}

func (t *topic) loadLine(lineName string, lineStoreValue lineStore) (*line, error) {
	// log.Printf("loading inflights: %v", lineStoreValue.Inflights)
	l := new(line)
	l.name = lineName
	l.headKey = t.name + "/" + lineName + KeyLineHead
	lineHeadData, err := t.q.storage.Get(l.headKey)
	if err != nil {
		return nil, NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	l.head = binary.LittleEndian.Uint64(lineHeadData)
	l.recycleKey = t.name + "/" + lineName + KeyLineRecycle
	lineRecycleData, err := t.q.storage.Get(l.recycleKey)
	if err != nil {
		return nil, NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	lineRecycle, err := time.ParseDuration(string(lineRecycleData))
	if err != nil {
		return nil, NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	l.recycle = lineRecycle
	l.ihead = lineStoreValue.Ihead
	imap := make(map[uint64]bool)
	for i := l.ihead; i < l.head; i++ {
		imap[i] = false
	}
	l.imap = imap
	inflight := list.New()
	for index, _ := range lineStoreValue.Inflights {
		msg := &lineStoreValue.Inflights[index]
		inflight.PushBack(msg)
		// inflight.PushBack(&lineStoreValue.Inflights[index])
		imap[msg.Tid] = true
	}
	l.inflight = inflight
	l.t = t

	err = t.q.registerLine(t.name, l.name, l.recycle.String())
	if err != nil {
		log.Printf("register line error: %s", err)
	}
	return l, nil
}

func (t *topic) push(data []byte) error {
	t.tailLock.Lock()
	defer t.tailLock.Unlock()

	key := Acatui(t.name, ":", t.tail)
	err := t.q.setData(key, data)
	if err != nil {
		return err
	}
	// log.Printf("key[%s][%s] pushed.", key, string(data))

	t.tail++
	err = t.exportTail()
	if err != nil {
		t.tail--
		return err
	}

	return nil
}

func (t *topic) mPush(datas [][]byte) error {
	t.tailLock.Lock()
	defer t.tailLock.Unlock()

	oldTail := t.tail
	for _, data := range datas {
		key := Acatui(t.name, ":", t.tail)
		err := t.q.setData(key, data)
		if err != nil {
			t.tail = oldTail
			return err
		}
		// log.Printf("key[%s][%s] pushed.", key, string(data))
		t.tail++
	}

	err := t.exportTail()
	if err != nil {
		t.tail = oldTail
		return err
	}

	return nil
}

func (t *topic) pop(name string) (uint64, []byte, error) {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return 0, nil, NewError(
			ErrLineNotExisted,
			`topic pop`,
		)
	}

	return l.pop()
}

func (t *topic) mPop(name string, n int) ([]uint64, [][]byte, error) {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return nil, nil, NewError(
			ErrLineNotExisted,
			`topic mPop`,
		)
	}

	return l.mPop(n)
}

func (t *topic) confirm(name string, id uint64) error {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return NewError(
			ErrLineNotExisted,
			`topic confirm`,
		)
	}

	return l.confirm(id)
}

func (t *topic) mConfirm(name string, ids []uint64) (int, error) {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return 0, NewError(
			ErrLineNotExisted,
			`topic mConfirm`,
		)
	}

	return l.mConfirm(ids)
}

func (t *topic) getData(id uint64) ([]byte, error) {
	key := Acatui(t.name, ":", id)
	return t.q.getData(key)
}

func (t *topic) backgroundClean() {
	t.wg.Add(1)
	defer t.wg.Done()

	bgQuit := false
	backupTick := time.NewTicker(BgBackupInterval)
	cleanTick := time.NewTicker(BgCleanInterval)
	for !bgQuit {
		select {
		case <-backupTick.C:
			err := t.exportLines()
			if err != nil {
				log.Printf("export topic[%s] lines error: %s", t.name, err)
			}
		case <-cleanTick.C:
			bgQuit := t.clean()
			if bgQuit {
				log.Printf("t.clean return quit: %v", bgQuit)
				break
			}
		case <-t.quit:
			log.Printf("background clean catched quit")
			bgQuit = true
			break
		}
	}
	log.Printf("background clean exit.")
}

func (t *topic) exportLines() error {
	t.linesLock.RLock()
	defer t.linesLock.RUnlock()

	for lineName, l := range t.lines {
		err := l.exportLine()
		if err != nil {
			log.Printf("line[%s] export error: %s", lineName, err)
			continue
		}
	}

	// log.Printf("topic[%s]'s all lines exported.", t.name)
	return nil
}

func (t *topic) clean() (quit bool) {
	quit = false

	t.headLock.RLock()
	defer t.headLock.RUnlock()

	starting := t.head
	endTime := time.Now().Add(BgCleanTimeout)
	// log.Printf("topic[%s] begin to clean at %d", t.name, starting)

	defer func() {
		if t.head != starting {
			log.Printf("garbage[%d - %d] are cleaned", starting, t.head)
		}
	}()

	ending := t.getEnd()
	for t.head < ending {
		select {
		case <-t.quit:
			quit = true
			log.Printf("catched quit at %d", t.head)
			return
		default:
			// nothing todo
		}

		if time.Now().After(endTime) {
			log.Printf("cleaning timeout, break at %d", t.head)
			return
		}

		key := Acatui(t.name, ":", t.head)
		err := t.q.delData(key)
		if err != nil {
			log.Printf("del data[%s] error; %s", key, err)
			return
		}

		t.head++
		err = t.exportHead()
		if err != nil {
			log.Printf("export topic[%s] head error: %s", t.name, err)
			return
		}
	}

	return
}

func (t *topic) getEnd() uint64 {
	var end uint64
	if len(t.lines) == 0 {
		end = t.head
	} else {
		end = t.tail
		for _, l := range t.lines {
			if l.recycle > 0 {
				if l.ihead < end {
					end = l.ihead
				}
			} else {
				if l.head < end {
					end = l.head
				}
			}
		}
	}
	return end
}

func (t *topic) close() {
	close(t.quit)
	t.wg.Wait()
}

func (t *topic) emptyLine(name string) error {
	t.linesLock.RLock()
	l, ok := t.lines[name]
	t.linesLock.RUnlock()
	if !ok {
		log.Printf("line[%s] not existed.", name)
		return NewError(
			ErrLineNotExisted,
			`topic emptyLine`,
		)
	}

	return l.empty()
}

func (t *topic) empty() error {
	t.linesLock.RLock()
	defer t.linesLock.RUnlock()

	for name, l := range t.lines {
		err := l.empty()
		if err != nil {
			log.Printf("line[%s] empty error: %s", name, err)
			return err
		}
	}

	t.headLock.Lock()
	defer t.headLock.Unlock()
	t.head = t.tail
	err := t.exportHead()
	if err != nil {
		return err
	}

	log.Printf("topic[%s] empty succ", t.name)
	return nil
}
