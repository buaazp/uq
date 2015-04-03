package queue

import "time"

type QueueRequest struct {
	TopicName string
	LineName  string
	Recycle   time.Duration
}

type MessageQueue interface {
	Create(cr *QueueRequest) error
	Push(name string, data []byte) error
	MultiPush(name string, datas [][]byte) error
	Pop(name string) (uint64, []byte, error)
	MultiPop(name string, n int) ([]uint64, [][]byte, error)
	Confirm(key string) error
	MultiConfirm(name string, ids []uint64) (int, error)
	Empty(cr *QueueRequest) error
	Close()
}
