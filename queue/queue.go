package queue

type MessageQueue interface {
	// queue functions
	Push(key string, data []byte) error
	MultiPush(key string, datas [][]byte) error
	Pop(key string) (string, []byte, error)
	MultiPop(key string, n int) ([]string, [][]byte, error)
	Confirm(key string) error
	MultiConfirm(keys []string) []error
	// admin functions
	Create(key, recycle string) error
	Empty(key string) error
	Remove(key string) error
	Stat(key string) (*QueueStat, error)
	Close()
}
