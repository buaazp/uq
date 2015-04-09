package entry

import (
	"log"
	"strings"
	"time"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

func (r *RedisEntry) OnQadd(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	arg := cmd.StringAtIndex(2)

	qr := new(queue.QueueRequest)
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		qr.TopicName = parts[0]
		qr.LineName = parts[1]
		if arg != "" {
			recycle, err := time.ParseDuration(arg)
			if err != nil {
				return ErrorReply(NewError(
					ErrBadRequest,
					err.Error(),
				))
			}
			qr.Recycle = recycle
		}
	} else if len(parts) == 1 {
		qr.TopicName = parts[0]
	} else {
		return ErrorReply(NewError(
			ErrBadKey,
			`key parts error: `+ItoaQuick(len(parts)),
		))
	}

	err := r.messageQueue.Create(qr)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnQpush(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	val, err := cmd.ArgAtIndex(2)
	if err != nil {
		return ErrorReply(NewError(
			ErrBadRequest,
			err.Error(),
		))
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
	vals[0] = id
	vals[1] = value

	return MultiBulksReply(vals)
}

func (r *RedisEntry) OnQmpop(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	n, err := cmd.IntAtIndex(2)
	if err != nil {
		return ErrorReply(NewError(
			ErrBadRequest,
			err.Error(),
		))
	}

	ids, values, err := r.messageQueue.MultiPop(key, n)
	if err != nil {
		return ErrorReply(err)
	}

	np := len(ids)

	vals := make([]interface{}, np*2)
	for i, index := 0, 0; i < np; i++ {
		vals[index] = ids[i]
		index++

		vals[index] = values[i]
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
			return ErrorReply(NewError(
				ErrBadRequest,
				err.Error(),
			))
		}
	}
	// log.Printf("key: %s ids: %v", key, ids)

	n, err := r.messageQueue.MultiConfirm(key, ids)
	if err != nil {
		log.Printf("confirm error: %s", err)
		return ErrorReply(err)
	}

	return IntegerReply(n)
}

func (r *RedisEntry) OnQempty(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)

	qr := new(queue.QueueRequest)
	parts := strings.Split(key, "/")
	if len(parts) == 2 {
		qr.TopicName = parts[0]
		qr.LineName = parts[1]
	} else if len(parts) == 1 {
		qr.TopicName = parts[0]
	} else {
		return ErrorReply(NewError(
			ErrBadKey,
			`key parts error: `+ItoaQuick(len(parts)),
		))
	}

	err := r.messageQueue.Empty(qr)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}
