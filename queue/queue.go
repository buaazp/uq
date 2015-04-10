package queue

type QueueRequest struct {
	TopicName string `json:"topic"`
	LineName  string `json:"line,omitempty"`
	Recycle   string `json:"recycle,omitempty"`
}

type MessageQueue interface {
	Create(qr *QueueRequest) error
	Push(key string, data []byte) error
	MultiPush(key string, datas [][]byte) error
	Pop(key string) (uint64, []byte, error)
	MultiPop(key string, n int) ([]uint64, [][]byte, error)
	Confirm(key string) error
	MultiConfirm(key string, ids []uint64) (int, error)
	Empty(key string) error
	Close()
}
