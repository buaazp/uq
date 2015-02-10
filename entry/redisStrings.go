package entry

// TODO 严谨的情况下应该校验参数数量，这里大部分都不校验是为了简化代码，panic后会断开client connection

var maxCmdLock = 100

func (r *RedisEntry) OnGET(cmd *Command) (reply *Reply) {
	key, _ := cmd.ArgAtIndex(1)
	_, value, err := r.messageQueue.Pop(string(key))
	if err != nil {
		return ErrorReply(err)
	}
	return BulkReply(value)
}

func (r *RedisEntry) OnSET(cmd *Command) (reply *Reply) {
	key, _ := cmd.ArgAtIndex(1)
	val, _ := cmd.ArgAtIndex(2)
	err := r.messageQueue.Push(string(key), val)
	if err != nil {
		return ErrorReply(err)
	}
	return StatusReply("OK")
}

func (r *RedisEntry) OnMGET(cmd *Command) (reply *Reply) {
	var err error
	keys := cmd.Args()[1:]
	vals := make([]interface{}, len(keys))
	for i, key := range keys {
		_, vals[i], err = r.messageQueue.Pop(string(key))
		if err != nil {
			return ErrorReply(err)
		}
	}
	reply = MultiBulksReply(vals)
	return
}

func (r *RedisEntry) OnMSET(cmd *Command) (reply *Reply) {
	keyvals := cmd.Args()[1:]
	if len(keyvals)%2 != 0 {
		return ErrorReply(WrongArgumentCount)
	}
	for i, count := 0, len(keyvals); i < count; i += 2 {
		key := keyvals[i]
		val := keyvals[i+1]
		err := r.messageQueue.Push(string(key), val)
		if err != nil {
			return ErrorReply(err)
		}
	}
	return StatusReply("OK")
}
