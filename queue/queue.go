package queue

type MessageQueue interface {
	Create(name string) error
	Push(name string, data []byte) error
	Pop(name string) ([]byte, error)
	// Confirm(name string) error
	Close()
}
