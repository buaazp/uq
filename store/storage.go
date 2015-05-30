package store

const (
	errNotExisted     string = "Data Not Existed"
	errModeNotMatched string = "Storage Mode Not Matched"
)

// Storage is the storage of uq
type Storage interface {
	Set(key string, data []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
	Close() error
}
