package store

import (
	"github.com/DanielMorsing/rocksdb"
)

type RockStore struct {
	path string
	db   *rocksdb.DB
	ro   *rocksdb.ReadOptions
	wo   *rocksdb.WriteOptions
}

func NewRockStore(path string) (*RockStore, error) {
	option := rocksdb.NewOptions()
	// option.SetCache(rocksdb.NewLRUCache(3 << 30))
	option.SetCreateIfMissing(true)

	db, err := rocksdb.Open(path, option)
	if err != nil {
		return nil, err
	}

	ro := rocksdb.NewReadOptions()
	wo := rocksdb.NewWriteOptions()

	rs := new(RockStore)
	rs.path = path
	rs.db = db
	rs.ro = ro
	rs.wo = wo

	return rs, nil
}

func (r *RockStore) Set(key string, data []byte) error {
	return r.db.Put(r.wo, []byte(key), data)
}

func (r *RockStore) Get(key string) ([]byte, error) {
	return r.db.Get(r.ro, []byte(key))
}

func (r *RockStore) Del(key string) error {
	return r.db.Delete(r.wo, []byte(key))
}

func (r *RockStore) Close() error {
	r.db.Close()
	return nil
}
