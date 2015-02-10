package entry

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/buaazp/uq/queue"
)

func (r *RedisEntry) OnPING(cmd *Command) (reply *Reply) {
	reply = StatusReply("PONG")
	return
}

func (r *RedisEntry) OnKEYS(cmd *Command) (reply *Reply) {
	return ErrorReply("Topic Names TODO")
}

// 官方redis的dbsize输出key数量，这里输出数据库大小
func (r *RedisEntry) OnDBSIZE(cmd *Command) (reply *Reply) {
	return StatusReply("TODO")
}

/**
 * 过期时间，暂不支持
 * 1 if the timeout was set.
 * 0 if key does not exist or the timeout could not be set.
 */
func (r *RedisEntry) OnEXPIRE(cmd *Command) (reply *Reply) {
	reply = IntegerReply(0)
	return
}

func (r *RedisEntry) OnADD(cmd *Command) (reply *Reply) {
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

func (r *RedisEntry) OnDEL(cmd *Command) (reply *Reply) {
	keys := cmd.Args()[1:]
	var n int = 0
	for _, key := range keys {
		cr := new(queue.ConfirmRequest)
		parts := strings.Split(string(key), "/")
		if len(parts) != 3 {
			continue
		} else {
			cr.TopicName = parts[0]
			cr.LineName = parts[1]
			id, err := strconv.ParseUint(parts[2], 10, 0)
			if err != nil {
				continue
			}
			cr.ID = id
		}
		err := r.messageQueue.Confirm(cr)
		if err != nil {
			log.Printf("confirm error: %s", err)
			continue
		} else {
			n++
		}
	}
	reply = IntegerReply(n)
	return
}

func (r *RedisEntry) OnTYPE(cmd *Command) (reply *Reply) {
	reply = StatusReply("none")
	return
}
