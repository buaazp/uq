package queue

import "time"

type message struct {
	tid uint64
}

type inflightMessage struct {
	Tid     uint64
	Exptime time.Time
}
