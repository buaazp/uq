package store

import (
	"errors"
	"sync"
)

type MemStore struct {
	mu sync.RWMutex
	db map[string][]byte
}

func NewMemStore() (*MemStore, error) {
	db := make(map[string][]byte)
	ms := new(MemStore)
	ms.db = db

	return ms, nil
}

func (m *MemStore) Get(key string) ([]byte, error) {
	data, ok := m.db[key]
	if !ok {
		return nil, errors.New(ErrNotExisted)
	}
	return data, nil
}

func (m *MemStore) Set(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.db[key] = data
	return nil
}

func (m *MemStore) Del(key string) error {
	_, ok := m.db[key]
	if !ok {
		return errors.New(ErrNotExisted)
	}

	delete(m.db, key)
	return nil
}

func (m *MemStore) Close() {
}
