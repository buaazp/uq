package entry

const (
	MaxKeyLength  int = 512
	MaxBodyLength int = 10 * 1024 * 1024
)

type Entrance interface {
	ListenAndServe() error
	Stop()
}
