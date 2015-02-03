package store

const (
	ErrNotExisted     string = "Data Not Existed"
	ErrModeNotMatched string = "Storage Mode Not Matched"
)

type Storage interface {
	Set(key string, data []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
	Close()
}
