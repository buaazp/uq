package queue

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buaazp/uq/store"
	. "github.com/buaazp/uq/utils"
	"github.com/coreos/go-etcd/etcd"
)

func init() {
	gob.Register(&unitedQueueStore{})
}

const (
	StorageKeyWord   string        = "UnitedQueueKey"
	BgBackupInterval time.Duration = 10 * time.Second
	BgCleanInterval  time.Duration = 20 * time.Second
	BgCleanTimeout   time.Duration = 5 * time.Second
	KeyTopicStore    string        = ":store"
	KeyTopicHead     string        = ":head"
	KeyTopicTail     string        = ":tail"
	KeyLineStore     string        = ":store"
	KeyLineHead      string        = ":head"
	KeyLineRecycle   string        = ":recycle"
	KeyLineInflight  string        = ":inflight"
)

type UnitedQueue struct {
	topics     map[string]*topic
	topicsLock sync.RWMutex
	storage    store.Storage
	etcdLock   sync.RWMutex
	selfAddr   string
	etcdClient *etcd.Client
	etcdKey    string
	etcdStop   chan bool
	wg         sync.WaitGroup
}

type unitedQueueStore struct {
	Topics []string
}

func NewUnitedQueue(storage store.Storage, ip string, port int, etcdServers []string, etcdKey string) (*UnitedQueue, error) {
	topics := make(map[string]*topic)
	etcdStop := make(chan bool)
	uq := new(UnitedQueue)
	uq.topics = topics
	uq.storage = storage
	uq.etcdStop = etcdStop

	if len(etcdServers) > 0 {
		selfAddr := Addrcat(ip, port)
		uq.selfAddr = selfAddr
		etcdClient := etcd.NewClient(etcdServers)
		uq.etcdClient = etcdClient
		uq.etcdKey = etcdKey
	}

	err := uq.loadQueue()
	if err != nil {
		return nil, err
	}

	go uq.etcdRun()
	return uq, nil
}

func (u *UnitedQueue) loadQueue() error {
	unitedQueueStoreData, err := u.getData(StorageKeyWord)
	if err != nil {
		log.Printf("storage not existed: %s", err)
		return nil
	}

	if len(unitedQueueStoreData) > 0 {
		var unitedQueueStoreValue unitedQueueStore
		dec := gob.NewDecoder(bytes.NewBuffer(unitedQueueStoreData))
		if e := dec.Decode(&unitedQueueStoreValue); e == nil {
			for _, topicName := range unitedQueueStoreValue.Topics {
				topicStoreData, err := u.getData(topicName)
				if err != nil || len(topicStoreData) == 0 {
					continue
				}
				var topicStoreValue topicStore
				dec2 := gob.NewDecoder(bytes.NewBuffer(topicStoreData))
				if e := dec2.Decode(&topicStoreValue); e == nil {
					t, err := u.loadTopic(topicName, topicStoreValue)
					if err != nil {
						continue
					}
					u.topicsLock.Lock()
					u.topics[topicName] = t
					u.topicsLock.Unlock()
				}
			}
		}
	}

	log.Printf("united queue load finisded.")
	log.Printf("u.topics: %v", u.topics)
	return nil
}

func (u *UnitedQueue) loadTopic(topicName string, topicStoreValue topicStore) (*topic, error) {
	t := new(topic)
	t.name = topicName
	t.q = u
	t.quit = make(chan bool)

	t.headKey = topicName + KeyTopicHead
	topicHeadData, err := u.getData(t.headKey)
	if err != nil {
		return nil, err
	}
	t.head = binary.LittleEndian.Uint64(topicHeadData)
	t.tailKey = topicName + KeyTopicTail
	topicTailData, err := u.getData(t.tailKey)
	if err != nil {
		return nil, err
	}
	t.tail = binary.LittleEndian.Uint64(topicTailData)

	lines := make(map[string]*line)
	for _, lineName := range topicStoreValue.Lines {
		lineStoreKey := topicName + "/" + lineName
		lineStoreData, err := u.getData(lineStoreKey)
		if err != nil || len(lineStoreData) == 0 {
			continue
		}
		var lineStoreValue lineStore
		dec3 := gob.NewDecoder(bytes.NewBuffer(lineStoreData))
		if e := dec3.Decode(&lineStoreValue); e == nil {
			l, err := t.loadLine(lineName, lineStoreValue)
			if err != nil {
				continue
			}
			lines[lineName] = l
			log.Printf("line[%s] load succ.", lineStoreKey)
		}
	}
	t.lines = lines

	err = u.registerTopic(t.name)
	if err != nil {
		log.Printf("register topic error: %s", err)
	}

	t.start()
	log.Printf("topic[%s] load succ.", topicName)
	log.Printf("topic: %v", t)
	return t, nil
}

func (u *UnitedQueue) Create(key, rec string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	var err error
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return NewError(
			ErrBadKey,
			`create key parts error: `+ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return NewError(
			ErrBadKey,
			`create topic is nil`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		var recycle time.Duration
		if rec != "" {
			recycle, err = time.ParseDuration(rec)
			if err != nil {
				return NewError(
					ErrBadRequest,
					err.Error(),
				)
			}
		}

		u.topicsLock.RLock()
		t, ok := u.topics[topicName]
		u.topicsLock.RUnlock()
		if !ok {
			return NewError(
				ErrTopicNotExisted,
				`queue create`,
			)
		}

		err = t.createLine(lineName, recycle, false)
		if err != nil {
			log.Printf("create line[%s] error: %s", lineName, err)
			return err
		}
	} else {
		err = u.createTopic(topicName, false)
		if err != nil {
			log.Printf("create topic[%s] error: %s", topicName, err)
			return err
		}
	}

	return err
}

func (u *UnitedQueue) createTopic(name string, fromEtcd bool) error {
	u.topicsLock.RLock()
	_, ok := u.topics[name]
	u.topicsLock.RUnlock()
	if ok {
		return NewError(
			ErrTopicExisted,
			`queue createTopic`,
		)
	}

	t, err := u.newTopic(name)
	if err != nil {
		return err
	}

	u.topicsLock.Lock()
	defer u.topicsLock.Unlock()
	u.topics[name] = t

	err = u.exportQueue()
	if err != nil {
		t.remove()
		delete(u.topics, name)
		return err
	}

	if !fromEtcd {
		err = u.registerTopic(t.name)
		if err != nil {
			log.Printf("register topic error: %s", err)
		}
	}
	log.Printf("topic[%s] created.", name)
	return nil
}

func (u *UnitedQueue) newTopic(name string) (*topic, error) {
	lines := make(map[string]*line)
	t := new(topic)
	t.name = name
	t.lines = lines
	t.head = 0
	t.headKey = name + KeyTopicHead
	t.tail = 0
	t.tailKey = name + KeyTopicTail
	t.q = u
	t.quit = make(chan bool)

	err := t.exportHead()
	if err != nil {
		return nil, err
	}
	err = t.exportTail()
	if err != nil {
		return nil, err
	}

	t.start()
	return t, nil
}

func (u *UnitedQueue) exportQueue() error {
	log.Printf("start export queue...")

	queueStoreValue, err := u.genQueueStore()
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(queueStoreValue)
	if err != nil {
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}

	err = u.setData(StorageKeyWord, buffer.Bytes())
	if err != nil {
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}

	log.Printf("united queue export finisded.")
	return nil
}

func (u *UnitedQueue) genQueueStore() (*unitedQueueStore, error) {
	topics := make([]string, len(u.topics))
	i := 0
	for topicName, _ := range u.topics {
		topics[i] = topicName
		i++
	}

	qs := new(unitedQueueStore)
	qs.Topics = topics
	return qs, nil
}

func (u *UnitedQueue) Push(key string, data []byte) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	if len(data) <= 0 {
		return NewError(
			ErrBadRequest,
			`message has no content`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[key]
	u.topicsLock.RUnlock()
	if !ok {
		return NewError(
			ErrTopicNotExisted,
			`queue push`,
		)
	}

	return t.push(data)
}

func (u *UnitedQueue) MultiPush(key string, datas [][]byte) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	u.topicsLock.RLock()
	t, ok := u.topics[key]
	u.topicsLock.RUnlock()
	if !ok {
		return NewError(
			ErrTopicNotExisted,
			`queue multiPush`,
		)
	}

	return t.mPush(datas)
}

func (u *UnitedQueue) Pop(key string) (string, []byte, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return "", nil, NewError(
			ErrBadKey,
			`pop key parts error: `+ItoaQuick(len(parts)),
		)
	}

	tName := parts[0]
	lName := parts[1]

	u.topicsLock.RLock()
	t, ok := u.topics[tName]
	u.topicsLock.RUnlock()
	if !ok {
		log.Printf("topic[%s] not existed.", tName)
		return "", nil, NewError(
			ErrTopicNotExisted,
			`queue pop`,
		)
	}

	id, data, err := t.pop(lName)
	if err != nil {
		return "", nil, err
	}

	return Acatui(key, "/", id), data, nil
}

func (u *UnitedQueue) MultiPop(key string, n int) ([]string, [][]byte, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return nil, nil, NewError(
			ErrBadKey,
			`mPop key parts error: `+ItoaQuick(len(parts)),
		)
	}

	tName := parts[0]
	lName := parts[1]

	u.topicsLock.RLock()
	t, ok := u.topics[tName]
	u.topicsLock.RUnlock()
	if !ok {
		log.Printf("topic[%s] not existed.", tName)
		return nil, nil, NewError(
			ErrTopicNotExisted,
			`queue multiPop`,
		)
	}

	ids, datas, err := t.mPop(lName, n)
	if err != nil {
		return nil, nil, err
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = Acatui(key, "/", id)
	}
	return keys, datas, nil
}

func (u *UnitedQueue) Confirm(key string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	var id uint64
	var err error
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return NewError(
			ErrBadKey,
			`confirm key parts error: `+ItoaQuick(len(parts)),
		)
	} else {
		topicName = parts[0]
		lineName = parts[1]
		id, err = strconv.ParseUint(parts[2], 10, 0)
		if err != nil {
			return NewError(
				ErrBadKey,
				`confirm key parse id error: `+err.Error(),
			)
		}
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		log.Printf("topic[%s] not existed.", topicName)
		return NewError(
			ErrTopicNotExisted,
			`queue confirm`,
		)
	}

	return t.confirm(lineName, id)
}

func (u *UnitedQueue) MultiConfirm(keys []string) []error {
	errs := make([]error, len(keys))
	for i, key := range keys {
		errs[i] = u.Confirm(key)
	}
	return errs
}

func (u *UnitedQueue) Empty(key string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	var err error
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return NewError(
			ErrBadKey,
			`empty key parts error: `+ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]

	if topicName == "" {
		return NewError(
			ErrBadKey,
			`empty topic is nil`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return NewError(
			ErrTopicNotExisted,
			`queue empty`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		err = t.emptyLine(lineName)
		if err != nil {
			log.Printf("empty line[%s] error: %s", lineName, err)
		}
		return err
	}

	return t.empty()
}

func (u *UnitedQueue) Remove(key string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return NewError(
			ErrBadKey,
			`remove key parts error: `+ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return NewError(
			ErrBadKey,
			`rmove topic is nil`,
		)
	}

	if len(parts) == 1 {
		return u.removeTopic(topicName)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return NewError(
			ErrTopicNotExisted,
			`queue remove`,
		)
	}

	lineName = parts[1]
	return t.removeLine(lineName)
}

func (u *UnitedQueue) removeTopic(name string) error {
	u.topicsLock.Lock()
	defer u.topicsLock.Unlock()

	t, ok := u.topics[name]
	if !ok {
		return NewError(
			ErrTopicNotExisted,
			`queue remove`,
		)
	}

	err := u.unRegisterTopic(name)
	if err != nil {
		return err
	}

	delete(u.topics, name)
	err = u.exportQueue()
	if err != nil {
		u.topics[name] = t
		return err
	}

	return t.remove()
}

func (u *UnitedQueue) Stat(key string) (*QueueStat, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return nil, NewError(
			ErrBadKey,
			`empty key parts error: `+ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return nil, NewError(
			ErrBadKey,
			`stat topic is nil`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return nil, NewError(
			ErrTopicNotExisted,
			`queue stat`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		return t.statLine(lineName)
	}

	return t.stat()
}

func (u *UnitedQueue) setData(key string, data []byte) error {
	err := u.storage.Set(key, data)
	if err != nil {
		log.Printf("key[%s] set data error: %s", key, err)
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	return nil
}

func (u *UnitedQueue) getData(key string) ([]byte, error) {
	data, err := u.storage.Get(key)
	if err != nil {
		log.Printf("key[%s] get data error: %s", key, err)
		return nil, NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	return data, nil
}

func (u *UnitedQueue) delData(key string) error {
	err := u.storage.Del(key)
	if err != nil {
		log.Printf("key[%s] del data error: %s", key, err)
		return NewError(
			ErrInternalError,
			err.Error(),
		)
	}
	return nil
}

func (u *UnitedQueue) Close() {
	log.Printf("uq stoping...")
	close(u.etcdStop)
	u.wg.Wait()

	for _, t := range u.topics {
		t.close()
	}

	err := u.exportTopics()
	if err != nil {
		log.Printf("export queue error: %s", err)
	}

	u.storage.Close()
}

func (u *UnitedQueue) exportTopics() error {
	u.topicsLock.RLock()
	defer u.topicsLock.RUnlock()

	for _, t := range u.topics {
		err := t.exportLines()
		if err != nil {
			log.Printf("topic[%s] export lines error: %s", t.name, err)
			continue
		}
		err = t.exportTopic()
		if err != nil {
			log.Printf("topic[%s] export error: %s", t.name, err)
			continue
		}
	}

	log.Printf("export all topics succ.")
	return nil
}
