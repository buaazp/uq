package entry

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

func (r *RedisEntry) OnQadd(cmd *Command) *Reply {
	key, _ := cmd.ArgAtIndex(1)
	val, _ := cmd.ArgAtIndex(2)

	cr := new(queue.CreateRequest)
	parts := strings.Split(string(key), "/")
	if len(parts) == 2 {
		cr.TopicName = parts[0]
		cr.LineName = parts[1]
		if len(val) > 0 {
			data := string(val)
			recycle, err := time.ParseDuration(data)
			if err != nil {
				return ErrorReply(err)
			}
			cr.Recycle = recycle
		}
	} else if len(parts) == 1 {
		cr.TopicName = parts[0]
	} else {
		return ErrorReply("CLIENT_ERROR client format")
	}

	err := r.messageQueue.Create(cr)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnQpush(cmd *Command) *Reply {
	key, _ := cmd.ArgAtIndex(1)
	val, _ := cmd.ArgAtIndex(2)
	err := r.messageQueue.Push(string(key), val)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnQpop(cmd *Command) *Reply {
	key, _ := cmd.ArgAtIndex(1)
	_, value, err := r.messageQueue.Pop(string(key))
	if err != nil {
		return ErrorReply(err)
	}
	return BulkReply(value)
}

func (r *RedisEntry) OnQmpop(cmd *Command) *Reply {
	key, _ := cmd.ArgAtIndex(1)

	id, value, err := r.messageQueue.Pop(string(key))
	if err != nil {
		return ErrorReply(err)
	}

	vals := make([]interface{}, 2)
	vals[0] = value

	confirmId := Acati(string(key), "/", id)
	vals[1] = confirmId

	return MultiBulksReply(vals)
}

func (r *RedisEntry) OnQdel(cmd *Command) *Reply {
	key, _ := cmd.ArgAtIndex(1)

	cr := new(queue.ConfirmRequest)
	parts := strings.Split(string(key), "/")
	if len(parts) != 3 {
		return ErrorReply(ERR_C_FORMAT)
	} else {
		cr.TopicName = parts[0]
		cr.LineName = parts[1]
		id, err := strconv.ParseUint(parts[2], 10, 0)
		if err != nil {
			return ErrorReply(ERR_C_FORMAT)
		}
		cr.ID = id
	}

	err := r.messageQueue.Confirm(cr)
	if err != nil {
		log.Printf("confirm error: %s", err)
		return ErrorReply(err)
	}

	return StatusReply("DELETE OK")
}
