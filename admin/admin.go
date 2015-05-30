package admin

// Administrator is the admin interface of uq
type Administrator interface {
	ListenAndServe() error
	Stop()
}
