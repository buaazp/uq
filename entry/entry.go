package entry

const (
	// MaxKeyLength is the max length of a key
	MaxKeyLength int = 512
	// MaxBodyLength is the max body length of a request
	MaxBodyLength int = 10 * 1024 * 1024
)

// Entrance is the interface of uq's entrance
type Entrance interface {
	ListenAndServe() error
	Stop()
}
