package entry

import (
	"github.com/buaazp/uq/utils"
)

func (r *RedisEntry) onQadd(cmd *command) *reply {
	key := cmd.stringAtIndex(1)
	recycle := cmd.stringAtIndex(2)

	// log.Printf("creating... %s %s", key, recycle)
	err := r.messageQueue.Create(key, recycle)
	if err != nil {
		return errorReply(err)
	}
	return statusReply("OK")
}

func (r *RedisEntry) onQpush(cmd *command) *reply {
	key := cmd.stringAtIndex(1)
	val, err := cmd.argAtIndex(2)
	if err != nil {
		return errorReply(utils.NewError(
			utils.ErrBadRequest,
			err.Error(),
		))
	}

	err = r.messageQueue.Push(key, val)
	if err != nil {
		return errorReply(err)
	}
	return statusReply("OK")
}

func (r *RedisEntry) onQmpush(cmd *command) *reply {
	key := cmd.stringAtIndex(1)
	vals := cmd.args[2:]

	err := r.messageQueue.MultiPush(key, vals)
	if err != nil {
		return errorReply(err)
	}

	return statusReply("OK")
}

func (r *RedisEntry) onQpop(cmd *command) *reply {
	key := cmd.stringAtIndex(1)

	id, value, err := r.messageQueue.Pop(key)
	if err != nil {
		return errorReply(err)
	}

	vals := make([]interface{}, 2)
	vals[0] = value
	vals[1] = id

	return multiBulksReply(vals)
}

func (r *RedisEntry) onQmpop(cmd *command) *reply {
	key := cmd.stringAtIndex(1)
	n, err := cmd.intAtIndex(2)
	if err != nil {
		return errorReply(utils.NewError(
			utils.ErrBadRequest,
			err.Error(),
		))
	}

	ids, values, err := r.messageQueue.MultiPop(key, n)
	if err != nil {
		return errorReply(err)
	}

	np := len(ids)

	vals := make([]interface{}, np*2)
	for i, index := 0, 0; i < np; i++ {
		vals[index] = values[i]
		index++

		vals[index] = ids[i]
		index++
	}

	return multiBulksReply(vals)
}

func (r *RedisEntry) onQdel(cmd *command) *reply {
	key := cmd.stringAtIndex(1)

	err := r.messageQueue.Confirm(key)
	if err != nil {
		// log.Printf("confirm error: %s", err)
		return errorReply(err)
	}

	return statusReply("OK")
}

func (r *RedisEntry) onQmdel(cmd *command) *reply {
	keys := cmd.stringArgs()[1:]
	// log.Printf("keys: %v", keys)

	errs := r.messageQueue.MultiConfirm(keys)

	vals := make([]interface{}, len(errs))
	for i, err := range errs {
		if err != nil {
			vals[i] = err.Error()
		} else {
			vals[i] = "OK"
		}
	}

	return multiBulksReply(vals)
}

func (r *RedisEntry) onQempty(cmd *command) *reply {
	key := cmd.stringAtIndex(1)

	err := r.messageQueue.Empty(key)
	if err != nil {
		return errorReply(err)
	}
	return statusReply("OK")
}

func (r *RedisEntry) onInfo(cmd *command) *reply {
	key := cmd.stringAtIndex(1)

	qs, err := r.messageQueue.Stat(key)
	if err != nil {
		return errorReply(err)
	}

	// for human reading
	strs := qs.ToRedisStrings()
	vals := make([]interface{}, len(strs))
	for i, str := range strs {
		vals[i] = str
	}
	return multiBulksReply(vals)

	// for json format
	// data, err := qs.ToJSON()
	// if err != nil {
	// 	return errorReply(utils.NewError(
	// 		utils.ErrInternalError,
	// 		err.Error(),
	// 	))
	// }

	// return statusReply(string(data))
}
