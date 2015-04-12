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
	option := &opt.Options{Compression: opt.SnappyCompression}
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
	return l.db.Put([]byte(key), data, nil)

	// err := l.db.Put(keyByte, data, nil)
	// if err != nil {
	// 	return err
	// }

	// return nil
}

func (l *LevelStore) Get(key string) ([]byte, error) {
	return l.db.Get([]byte(key), nil)

	// data, err := l.db.Get(keyByte, nil)
	// if err != nil {
	// 	return nil, err
	// }

	// return data, nil
}

func (l *LevelStore) Del(key string) error {
	return l.db.Delete([]byte(key), nil)

	// err := l.db.Delete(keyByte, nil)
	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (l *LevelStore) Close() error {
	err := l.db.Close()
	if err != nil {
		log.Printf("leveldb close error: %s", err)
		return err
	}
	return nil
}
