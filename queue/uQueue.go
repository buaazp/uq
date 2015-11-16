package queue

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buaazp/uq/store"
	"github.com/buaazp/uq/utils"
	"github.com/coreos/go-etcd/etcd"
)

func init() {
	gob.Register(&unitedQueueStore{})
}

const (
	storageKeyWord   string        = "UnitedQueueKey"
	bgBackupInterval time.Duration = 10 * time.Second
	bgCleanInterval  time.Duration = 20 * time.Second
	bgCleanTimeout   time.Duration = 5 * time.Second
	keyTopicStore    string        = ":store"
	keyTopicHead     string        = ":head"
	keyTopicTail     string        = ":tail"
	keyLineStore     string        = ":store"
	keyLineHead      string        = ":head"
	keyLineRecycle   string        = ":recycle"
	keyLineInflight  string        = ":inflight"
)

// UnitedQueue is a implemention of message queue in uq
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

// NewUnitedQueue returns a new UnitedQueue
func NewUnitedQueue(storage store.Storage, ip string, port int, etcdServers []string, etcdKey string) (*UnitedQueue, error) {
	topics := make(map[string]*topic)
	etcdStop := make(chan bool)
	uq := new(UnitedQueue)
	uq.topics = topics
	uq.storage = storage
	uq.etcdStop = etcdStop

	if len(etcdServers) > 0 {
		selfAddr := utils.Addrcat(ip, port)
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

func (u *UnitedQueue) setData(key string, data []byte) error {
	err := u.storage.Set(key, data)
	if err != nil {
		// log.Printf("key[%s] set data error: %s", key, err)
		return utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		)
	}
	return nil
}

func (u *UnitedQueue) getData(key string) ([]byte, error) {
	data, err := u.storage.Get(key)
	if err != nil {
		// log.Printf("key[%s] get data error: %s", key, err)
		return nil, utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		)
	}
	return data, nil
}

func (u *UnitedQueue) delData(key string) error {
	err := u.storage.Del(key)
	if err != nil {
		// log.Printf("key[%s] del data error: %s", key, err)
		return utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		)
	}
	return nil
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
		t.linesLock.RLock()
		err = t.exportTopic()
		t.linesLock.RUnlock()
		if err != nil {
			log.Printf("topic[%s] export error: %s", t.name, err)
			continue
		}
	}

	// log.Printf("export all topics succ.")
	return nil
}

func (u *UnitedQueue) genQueueStore() *unitedQueueStore {
	topics := make([]string, len(u.topics))
	i := 0
	for topicName := range u.topics {
		topics[i] = topicName
		i++
	}

	qs := new(unitedQueueStore)
	qs.Topics = topics
	return qs
}

func (u *UnitedQueue) exportQueue() error {
	// log.Printf("start export queue...")

	queueStoreValue := u.genQueueStore()

	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(queueStoreValue)
	if err != nil {
		return err
	}

	err = u.setData(storageKeyWord, buffer.Bytes())
	if err != nil {
		return err
	}

	// log.Printf("united queue export finisded.")
	return nil
}

func (u *UnitedQueue) loadTopic(topicName string, topicStoreValue topicStore) (*topic, error) {
	t := new(topic)
	t.name = topicName
	t.persist = topicStoreValue.Persist
	t.q = u
	t.quit = make(chan bool)

	t.headKey = topicName + keyTopicHead
	topicHeadData, err := u.getData(t.headKey)
	if err != nil {
		return nil, err
	}
	t.head = binary.LittleEndian.Uint64(topicHeadData)
	t.tailKey = topicName + keyTopicTail
	topicTailData, err := u.getData(t.tailKey)
	if err != nil {
		return nil, err
	}
	t.tail = binary.LittleEndian.Uint64(topicTailData)

	lines := make(map[string]*line)
	for _, lineName := range topicStoreValue.Lines {
		lineStoreKey := topicName + "/" + lineName
		lineStoreData, err := u.getData(lineStoreKey)
		if err != nil {
			return nil, err
		}
		if len(lineStoreData) == 0 {
			return nil, errors.New("line backup data missing: " + lineStoreKey)
		}
		var lineStoreValue lineStore
		dec3 := gob.NewDecoder(bytes.NewBuffer(lineStoreData))
		if e := dec3.Decode(&lineStoreValue); e == nil {
			l, err := t.loadLine(lineName, lineStoreValue)
			if err != nil {
				continue
			}
			lines[lineName] = l
			// log.Printf("line[%s] load succ.", lineStoreKey)
		}
	}
	t.lines = lines

	u.registerTopic(t.name)

	t.start()
	// log.Printf("topic[%s] load succ.", topicName)
	// log.Printf("topic: %v", t)
	return t, nil
}

func (u *UnitedQueue) loadQueue() error {
	unitedQueueStoreData, err := u.getData(storageKeyWord)
	if err != nil {
		// log.Printf("storage not existed: %s", err)
		return nil
	}

	if len(unitedQueueStoreData) > 0 {
		var unitedQueueStoreValue unitedQueueStore
		dec := gob.NewDecoder(bytes.NewBuffer(unitedQueueStoreData))
		if e := dec.Decode(&unitedQueueStoreValue); e == nil {
			for _, topicName := range unitedQueueStoreValue.Topics {
				topicStoreData, err := u.getData(topicName)
				if err != nil {
					return err
				}
				if len(topicStoreData) == 0 {
					return errors.New("topic backup data missing: " + topicName)
				}
				var topicStoreValue topicStore
				dec2 := gob.NewDecoder(bytes.NewBuffer(topicStoreData))
				if e := dec2.Decode(&topicStoreValue); e == nil {
					t, err := u.loadTopic(topicName, topicStoreValue)
					if err != nil {
						return err
					}
					u.topicsLock.Lock()
					u.topics[topicName] = t
					u.topicsLock.Unlock()
				}
			}
		}
	}

	// log.Printf("united queue load finisded.")
	// log.Printf("u.topics: %v", u.topics)
	return nil
}

func (u *UnitedQueue) newTopic(name string, persist bool) (*topic, error) {
	lines := make(map[string]*line)
	t := new(topic)
	t.name = name
	t.persist = persist
	t.lines = lines
	t.head = 0
	t.headKey = name + keyTopicHead
	t.tail = 0
	t.tailKey = name + keyTopicTail
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

func (u *UnitedQueue) createTopic(name string, persist, fromEtcd bool) error {
	u.topicsLock.RLock()
	_, ok := u.topics[name]
	u.topicsLock.RUnlock()
	if ok {
		return utils.NewError(
			utils.ErrTopicExisted,
			`queue createTopic`,
		)
	}

	t, err := u.newTopic(name, persist)
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
		u.registerTopic(t.name)
	}
	log.Printf("topic[%s] created.", name)
	return nil
}

func (u *UnitedQueue) create(key, arg string, fromEtcd bool) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	var err error
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return utils.NewError(
			utils.ErrBadKey,
			`create key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return utils.NewError(
			utils.ErrBadKey,
			`create topic is nil`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		var recycle time.Duration
		if arg != "" {
			recycle, err = time.ParseDuration(arg)
			if err != nil {
				return utils.NewError(
					utils.ErrBadRequest,
					err.Error(),
				)
			}
		}

		u.topicsLock.RLock()
		t, ok := u.topics[topicName]
		u.topicsLock.RUnlock()
		if !ok {
			return utils.NewError(
				utils.ErrTopicNotExisted,
				`queue create`,
			)
		}

		err = t.createLine(lineName, recycle, fromEtcd)
		if err != nil {
			// log.Printf("create line[%s] error: %s", lineName, err)
			return err
		}
	} else {
		var persist bool
		if arg == "persist" {
			persist = true
		}
		err = u.createTopic(topicName, persist, fromEtcd)
		if err != nil {
			// log.Printf("create topic[%s] error: %s", topicName, err)
			return err
		}
	}

	return err
}

// Create implements Create interface
func (u *UnitedQueue) Create(key, arg string) error {
	return u.create(key, arg, false)
}

// Push implements Push interface
func (u *UnitedQueue) Push(key string, data []byte) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	if len(data) <= 0 {
		return utils.NewError(
			utils.ErrBadRequest,
			`message has no content`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[key]
	u.topicsLock.RUnlock()
	if !ok {
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue push`,
		)
	}

	return t.push(data)
}

// MultiPush implements MultiPush interface
func (u *UnitedQueue) MultiPush(key string, datas [][]byte) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	for i, data := range datas {
		if len(data) <= 0 {
			cause := "message " + strconv.Itoa(i) + " has no content"
			return utils.NewError(
				utils.ErrBadRequest,
				cause,
			)
		}
	}

	u.topicsLock.RLock()
	t, ok := u.topics[key]
	u.topicsLock.RUnlock()
	if !ok {
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue multiPush`,
		)
	}

	return t.mPush(datas)
}

// Pop implements Pop interface
func (u *UnitedQueue) Pop(key string) (string, []byte, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return "", nil, utils.NewError(
			utils.ErrBadKey,
			`pop key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	tName := parts[0]
	lName := parts[1]

	u.topicsLock.RLock()
	t, ok := u.topics[tName]
	u.topicsLock.RUnlock()
	if !ok {
		// log.Printf("topic[%s] not existed.", tName)
		return "", nil, utils.NewError(
			utils.ErrTopicNotExisted,
			`queue pop`,
		)
	}

	id, data, err := t.pop(lName)
	if err != nil {
		return "", nil, err
	}

	return utils.Acatui(key, "/", id), data, nil
}

// MultiPop implements MultiPop interface
func (u *UnitedQueue) MultiPop(key string, n int) ([]string, [][]byte, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return nil, nil, utils.NewError(
			utils.ErrBadKey,
			`mPop key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	tName := parts[0]
	lName := parts[1]

	u.topicsLock.RLock()
	t, ok := u.topics[tName]
	u.topicsLock.RUnlock()
	if !ok {
		// log.Printf("topic[%s] not existed.", tName)
		return nil, nil, utils.NewError(
			utils.ErrTopicNotExisted,
			`queue multiPop`,
		)
	}

	ids, datas, err := t.mPop(lName, n)
	if err != nil {
		return nil, nil, err
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = utils.Acatui(key, "/", id)
	}
	return keys, datas, nil
}

// Confirm implements Confirm interface
func (u *UnitedQueue) Confirm(key string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return utils.NewError(
			utils.ErrBadKey,
			`confirm key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}
	topicName := parts[0]
	lineName := parts[1]
	id, err := strconv.ParseUint(parts[2], 10, 0)
	if err != nil {
		return utils.NewError(
			utils.ErrBadKey,
			`confirm key parse id error: `+err.Error(),
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		// log.Printf("topic[%s] not existed.", topicName)
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue confirm`,
		)
	}

	return t.confirm(lineName, id)
}

// MultiConfirm implements MultiConfirm interface
func (u *UnitedQueue) MultiConfirm(keys []string) []error {
	errs := make([]error, len(keys))
	for i, key := range keys {
		errs[i] = u.Confirm(key)
	}
	return errs
}

// Stat implements Stat interface
func (u *UnitedQueue) Stat(key string) (*Stat, error) {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return nil, utils.NewError(
			utils.ErrBadKey,
			`empty key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return nil, utils.NewError(
			utils.ErrBadKey,
			`stat topic is nil`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return nil, utils.NewError(
			utils.ErrTopicNotExisted,
			`queue stat`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		return t.statLine(lineName)
	}

	qs := t.stat()
	return qs, nil
}

// Empty implements Empty interface
func (u *UnitedQueue) Empty(key string) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return utils.NewError(
			utils.ErrBadKey,
			`empty key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]

	if topicName == "" {
		return utils.NewError(
			utils.ErrBadKey,
			`empty topic is nil`,
		)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue empty`,
		)
	}

	if len(parts) == 2 {
		lineName = parts[1]
		return t.emptyLine(lineName)
		// err = t.emptyLine(lineName)
		// if err != nil {
		// 	log.Printf("empty line[%s] error: %s", lineName, err)
		// }
		// return err
	}

	return t.empty()
}

func (u *UnitedQueue) removeTopic(name string, fromEtcd bool) error {
	u.topicsLock.Lock()
	defer u.topicsLock.Unlock()

	t, ok := u.topics[name]
	if !ok {
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue remove`,
		)
	}

	delete(u.topics, name)
	err := u.exportQueue()
	if err != nil {
		u.topics[name] = t
		return err
	}

	if !fromEtcd {
		u.unRegisterTopic(name)
	}

	return t.remove()
}

func (u *UnitedQueue) remove(key string, fromEtcd bool) error {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")

	var topicName, lineName string
	parts := strings.Split(key, "/")
	if len(parts) < 1 || len(parts) > 2 {
		return utils.NewError(
			utils.ErrBadKey,
			`remove key parts error: `+utils.ItoaQuick(len(parts)),
		)
	}

	topicName = parts[0]
	if topicName == "" {
		return utils.NewError(
			utils.ErrBadKey,
			`rmove topic is nil`,
		)
	}

	if len(parts) == 1 {
		return u.removeTopic(topicName, fromEtcd)
	}

	u.topicsLock.RLock()
	t, ok := u.topics[topicName]
	u.topicsLock.RUnlock()
	if !ok {
		return utils.NewError(
			utils.ErrTopicNotExisted,
			`queue remove`,
		)
	}

	lineName = parts[1]
	return t.removeLine(lineName, fromEtcd)
}

// Remove implements Remove interface
func (u *UnitedQueue) Remove(key string) error {
	return u.remove(key, false)
}

// Close implements Close interface
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
	log.Printf("uq stoped.")
}
