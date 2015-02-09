package store

import (
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelStore struct {
	path    string
	db      *leveldb.DB
	timeout time.Duration
}

func NewLevelStore(path string) (*LevelStore, error) {
	option := &opt.Options{Compression: opt.NoCompression}
	db, err := leveldb.OpenFile(path, option)
	if err != nil {
		return nil, err
	}
	ls := new(LevelStore)
	ls.path = path
	ls.db = db

	return ls, nil
}

func (l *LevelStore) Set(key string, data []byte) error {
	keyByte := []byte(key)
	return l.db.Put(keyByte, data, nil)

	// err := l.db.Put(keyByte, data, nil)
	// if err != nil {
	// 	return err
	// }

	// return nil
}

func (l *LevelStore) Get(key string) ([]byte, error) {
	keyByte := []byte(key)
	return l.db.Get(keyByte, nil)

	// data, err := l.db.Get(keyByte, nil)
	// if err != nil {
	// 	return nil, err
	// }

	// return data, nil
}

func (l *LevelStore) Del(key string) error {
	keyByte := []byte(key)
	return l.db.Delete(keyByte, nil)

	// err := l.db.Delete(keyByte, nil)
	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (l *LevelStore) Close() {
	err := l.db.Close()
	if err != nil {
		log.Printf("leveldb close error: %s", err)
	}
}
