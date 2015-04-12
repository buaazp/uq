package admin

type AdminServer interface {
	ListenAndServe() error
	Stop()
}
