package queue

type QueueStat struct {
	TopicName string       `json:"topic"`
	Lines     []*QueueStat `json:"lines,omitempty"`
	LineName  string       `json:"line,omitempty"`
	Recycle   string       `json:"recycle,omitempty"`
	Head      uint64       `json:"head"`
	IHead     uint64       `json:"ihead"`
	Tail      uint64       `json:"tail"`
	Count     uint64       `json:"count"`
}

type MessageQueue interface {
	Create(key, recycle string) error
	Push(key string, data []byte) error
	MultiPush(key string, datas [][]byte) error
	Pop(key string) (string, []byte, error)
	MultiPop(key string, n int) ([]string, [][]byte, error)
	Confirm(key string) error
	MultiConfirm(keys []string) []error
	Empty(key string) error
	Stat(key string) (*QueueStat, error)
	Close()
}
