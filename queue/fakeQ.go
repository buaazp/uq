package queue

import (
	"github.com/buaazp/uq/store"
)

type FakeQueue struct{}

func NewFakeQueue(storage store.Storage, ip string, port int, etcdServers []string, etcdKey string) (*FakeQueue, error) {
	fq := new(FakeQueue)
	return fq, nil
}

// queue functions
func (f *FakeQueue) Push(key string, data []byte) error {
	return nil
}

func (f *FakeQueue) MultiPush(key string, datas [][]byte) error {
	return nil
}

func (f *FakeQueue) Pop(key string) (string, []byte, error) {
	return "", nil, nil
}

func (f *FakeQueue) MultiPop(key string, n int) ([]string, [][]byte, error) {
	return nil, nil, nil
}

func (f *FakeQueue) Confirm(key string) error {
	return nil
}

func (f *FakeQueue) MultiConfirm(keys []string) []error {
	return nil
}

// admin functions
func (f *FakeQueue) Create(key, recycle string) error {
	return nil
}

func (f *FakeQueue) Empty(key string) error {
	return nil
}

func (f *FakeQueue) Remove(key string) error {
	return nil
}

func (f *FakeQueue) Stat(key string) (*QueueStat, error) {
	return nil, nil
}

func (f *FakeQueue) Close() {
	return
}
