package queue

import (
	"github.com/buaazp/uq/store"
)

// FakeQueue is a fake queue in uq
type FakeQueue struct{}

// NewFakeQueue returns a new FakeQueue
func NewFakeQueue(storage store.Storage, ip string, port int, etcdServers []string, etcdKey string) (*FakeQueue, error) {
	fq := new(FakeQueue)
	return fq, nil
}

// queue functions

// Push implements Push interface
func (f *FakeQueue) Push(key string, data []byte) error {
	return nil
}

// MultiPush implements MultiPush interface
func (f *FakeQueue) MultiPush(key string, datas [][]byte) error {
	return nil
}

// Pop implements Pop interface
func (f *FakeQueue) Pop(key string) (string, []byte, error) {
	return "", nil, nil
}

// MultiPop implements MultiPop interface
func (f *FakeQueue) MultiPop(key string, n int) ([]string, [][]byte, error) {
	return nil, nil, nil
}

// Confirm implements Confirm interface
func (f *FakeQueue) Confirm(key string) error {
	return nil
}

// MultiConfirm implements MultiConfirm interface
func (f *FakeQueue) MultiConfirm(keys []string) []error {
	return nil
}

// admin functions

// Create implements Create interface
func (f *FakeQueue) Create(key, recycle string) error {
	return nil
}

// Empty implements Empty interface
func (f *FakeQueue) Empty(key string) error {
	return nil
}

// Remove implements Remove interface
func (f *FakeQueue) Remove(key string) error {
	return nil
}

// Stat implements Stat interface
func (f *FakeQueue) Stat(key string) (*Stat, error) {
	return nil, nil
}

// Close implements Close interface
func (f *FakeQueue) Close() {
	return
}
