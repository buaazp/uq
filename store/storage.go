package store

const (
	ErrNotExisted     string = "Data Not Existed"
	ErrModeNotMatched string = "Storage Mode Not Matched"
)

type Storage interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte) error
	Close()
}
