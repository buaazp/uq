package entry

import (
	"log"

	. "github.com/buaazp/uq/utils"
)

func (r *RedisEntry) OnQadd(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)
	recycle := cmd.StringAtIndex(2)

	log.Printf("creating... %s %s", key, recycle)
	err := r.messageQueue.Create(key, recycle)
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
	vals[0] = value
	vals[1] = id

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
	keys := cmd.StringArgs()[1:]
	log.Printf("keys: %v", keys)

	errs := r.messageQueue.MultiConfirm(keys)

	vals := make([]interface{}, len(errs))
	for i, err := range errs {
		if err != nil {
			vals[i] = err.Error()
		} else {
			vals[i] = "OK"
		}
	}

	return MultiBulksReply(vals)
}

func (r *RedisEntry) OnQempty(cmd *Command) *Reply {
	key := cmd.StringAtIndex(1)

	err := r.messageQueue.Empty(key)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}
