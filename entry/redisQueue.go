package entry

import (
	"log"
	"strings"
	"time"

	"github.com/buaazp/uq/queue"
)

func (r *RedisEntry) OnQadd(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	arg := cmd.StringAtIndex(2)

	cr := new(queue.CreateRequest)
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		cr.TopicName = parts[0]
		cr.LineName = parts[1]
		if arg != "" {
			recycle, err := time.ParseDuration(arg)
			if err != nil {
				return ErrorReply(err)
			}
			cr.Recycle = recycle
		}
	} else if len(parts) == 1 {
		cr.TopicName = parts[0]
	} else {
		return ErrorReply("ERR bad key format '" + key + "'")
	}

	err := r.messageQueue.Create(cr)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnQpush(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	val, err := cmd.ArgAtIndex(2)
	if err != nil {
		return ErrorReply(err)
	}

	err = r.messageQueue.Push(key, val)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnQmpush(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	vals := cmd.Args()[2:]

	err := r.messageQueue.MultiPush(key, vals)
	if err != nil {
		return ErrorReply(err)
	}

	return StatusReply("OK")
}

func (r *RedisEntry) OnQpop(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)

	id, value, err := r.messageQueue.Pop(key)
	if err != nil {
		return ErrorReply(err)
	}

	vals := make([]interface{}, 2)
	vals[0] = value
	vals[1] = id

	return MultiBulksReply(vals)
}

func (r *RedisEntry) OnQmpop(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	n, err := cmd.IntAtIndex(2)
	if err != nil {
		return ErrorReply(err)
	}

	ids, values, err := r.messageQueue.MultiPop(key, n)
	if err != nil {
		return ErrorReply(err)
	}

	np := len(ids)

	vals := make([]interface{}, np*2)
	for i, index := 0, 0; i < np; i++ {
		vals[index] = values[i]
		index++

		vals[index] = ids[i]
		index++
	}

	return MultiBulksReply(vals)
}

func (r *RedisEntry) OnQdel(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)

	err := r.messageQueue.Confirm(key)
	if err != nil {
		log.Printf("confirm error: %s", err)
		return ErrorReply(err)
	}

	return StatusReply("OK")
}

func (r *RedisEntry) OnQmdel(cmd *Command) *Reply {
	var err error
	key := cmd.StringAtIndex(1)
	ids := make([]uint64, cmd.Len()-2)
	for i := 2; i < cmd.Len(); i++ {
		ids[i-2], err = cmd.Uint64AtIndex(i)
		if err != nil {
			return ErrorReply(err)
		}
	}
	log.Printf("key: %s ids: %v", key, ids)

	n, err := r.messageQueue.MultiConfirm(key, ids)
	if err != nil {
		log.Printf("confirm error: %s", err)
		return ErrorReply(err)
	}

	return IntegerReply(n)
}
