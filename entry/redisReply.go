package entry

import (
	"bytes"
	"fmt"
)

type Reply struct {
	Type  ReplyType
	Value interface{}
}

type ReplyType int

var NOREPLY *Reply = nil

const (
	ReplyTypeStatus ReplyType = iota
	ReplyTypeError
	ReplyTypeInteger
	ReplyTypeBulk
	ReplyTypeMultiBulks
)

var replyTypeDesc = map[ReplyType]string{
	ReplyTypeStatus:     "StatusReply",
	ReplyTypeError:      "ErrorReply",
	ReplyTypeInteger:    "IntegerReply",
	ReplyTypeBulk:       "BulkReply",
	ReplyTypeMultiBulks: "MultiBulksReply",
}

func StatusReply(status string) (r *Reply) {
	r = &Reply{}
	r.Type = ReplyTypeStatus
	r.Value = status
	return
}

func ErrorReply(err error) (r *Reply) {
	r = &Reply{}
	r.Type = ReplyTypeError
	if err != nil {
		r.Value = err.Error()
	}
	return
}

func IntegerReply(i int) (r *Reply) {
	r = &Reply{}
	r.Type = ReplyTypeInteger
	r.Value = i
	return
}

func BulkReply(bulk interface{}) (r *Reply) {
	r = &Reply{}
	r.Type = ReplyTypeBulk
	r.Value = bulk
	return
}

func MultiBulksReply(bulks []interface{}) (r *Reply) {
	r = &Reply{}
	r.Type = ReplyTypeMultiBulks
	r.Value = bulks
	return
}

func (r *Reply) String() string {
	buf := bytes.Buffer{}
	buf.WriteString("<")
	buf.WriteString(replyTypeDesc[r.Type])
	buf.WriteString(":")
	switch r.Value.(type) {
	case []byte:
		buf.WriteString(string(r.Value.([]byte)))
	default:
		buf.WriteString(fmt.Sprint(r.Value))
	}
	buf.WriteString(">")
	return buf.String()
}
