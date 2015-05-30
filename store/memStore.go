package store

import (
	"errors"
	"sync"
)

// MemStore is the in memory storage
type MemStore struct {
	mu sync.RWMutex
	db map[string][]byte
}

// NewMemStore returns a new MemStore
func NewMemStore() (*MemStore, error) {
	db := make(map[string][]byte)
	ms := new(MemStore)
	ms.db = db

	return ms, nil
}

// Set implements the Set interface
func (m *MemStore) Set(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db[key] = data
	return nil
}

// Get implements the Get interface
func (m *MemStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.db[key]
	if !ok {
		return nil, errors.New(errNotExisted)
	}
	return data, nil
}

// Del implements the Del interface
func (m *MemStore) Del(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.db[key]
	if !ok {
		return errors.New(errNotExisted)
	}

	delete(m.db, key)
	return nil
}

// Close implements the Close interface
func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db = nil
	return nil
}
