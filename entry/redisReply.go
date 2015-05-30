package entry

import (
	"bytes"
	"fmt"
)

type reply struct {
	rType replyType
	value interface{}
}

type replyType int

const (
	replyTypeStatus replyType = iota
	replyTypeError
	replyTypeInteger
	replyTypeBulk
	replyTypeMultiBulks
)

var replyTypeDesc = map[replyType]string{
	replyTypeStatus:     "statusReply",
	replyTypeError:      "errorReply",
	replyTypeInteger:    "integerReply",
	replyTypeBulk:       "bulkReply",
	replyTypeMultiBulks: "multiBulksReply",
}

func statusReply(status string) (r *reply) {
	r = &reply{}
	r.rType = replyTypeStatus
	r.value = status
	return
}

func errorReply(err error) (r *reply) {
	r = &reply{}
	r.rType = replyTypeError
	if err != nil {
		r.value = err.Error()
	}
	return
}

func integerReply(i int) (r *reply) {
	r = &reply{}
	r.rType = replyTypeInteger
	r.value = i
	return
}

func bulkReply(bulk interface{}) (r *reply) {
	r = &reply{}
	r.rType = replyTypeBulk
	r.value = bulk
	return
}

func multiBulksReply(bulks []interface{}) (r *reply) {
	r = &reply{}
	r.rType = replyTypeMultiBulks
	r.value = bulks
	return
}

func (r *reply) String() string {
	buf := bytes.Buffer{}
	buf.WriteString("<")
	buf.WriteString(replyTypeDesc[r.rType])
	buf.WriteString(":")
	switch r.value.(type) {
	case []byte:
		buf.WriteString(string(r.value.([]byte)))
	default:
		buf.WriteString(fmt.Sprint(r.value))
	}
	buf.WriteString(">")
	return buf.String()
}
