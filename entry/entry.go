package entry

type Entrance interface {
	ListenAndServe() error
	Stop()
}
