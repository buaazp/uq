package queue

import "time"

type CreateRequest struct {
	TopicName string
	LineName  string
	Recycle   time.Duration
}

type ConfirmRequest struct {
	TopicName string
	LineName  string
	ID        uint64
}

type MessageQueue interface {
	Create(cr *CreateRequest) error
	Push(name string, data []byte) error
	Pop(name string) (uint64, []byte, error)
	Confirm(cr *ConfirmRequest) error
	Close()
}
