package entry

import (
	"errors"
	"strings"
)

var (
	BadCommandError    = errors.New("bad command")
	WrongArgumentCount = errors.New("wrong argument count")
	WrongCommandKey    = errors.New("wrong command key")
)

const (
	RI_MinCount = iota
	RI_MaxCount // -1 for undefined
)

var cmdrules = map[string][]interface{}{
	// queue
	"QADD":   []interface{}{2, 3},
	"QPUSH":  []interface{}{3, 3},
	"QMPUSH": []interface{}{3, -1},
	"QPOP":   []interface{}{2, 2},
	"QMPOP":  []interface{}{3, -1},
	"QDEL":   []interface{}{2, 2},
	"QMDEL":  []interface{}{2, -1},
	"QEMPTY": []interface{}{2, 2},
	// key
	"DEL":     []interface{}{2, -1},
	"TYPE":    []interface{}{2, 2},
	"KEYNEXT": []interface{}{2, -1},
	// string
	"APPEND": []interface{}{3, 3},
	"GET":    []interface{}{2, 2},
	"SET":    []interface{}{3, -1},
	"MGET":   []interface{}{2, -1},
	"MSET":   []interface{}{3, -1},
	"INCR":   []interface{}{2, 2},
	"DECR":   []interface{}{2, 2},
	"INCRBY": []interface{}{3, 3},
	"DECRBY": []interface{}{3, 3},
	// hash
	"HGET":    []interface{}{3, 3},
	"HSET":    []interface{}{4, 4},
	"HMGET":   []interface{}{3, -1},
	"HMSET":   []interface{}{4, -1},
	"HGETALL": []interface{}{2, 2},
	"HLEN":    []interface{}{2, 2},
	"HDEL":    []interface{}{3, -1},
	// set
	"SADD":      []interface{}{3, -1},
	"SCARD":     []interface{}{2, 2},
	"SISMEMBER": []interface{}{3, 3},
	"SMEMBERS":  []interface{}{2, 2},
	"SREM":      []interface{}{3, -1},
	// list
	"LPUSH":  []interface{}{3, -1},
	"RPUSH":  []interface{}{3, -1},
	"LPOP":   []interface{}{2, 2},
	"RPOP":   []interface{}{2, 2},
	"LINDEX": []interface{}{3, 3},
	"LTRIM":  []interface{}{4, 4},
	"LRANGE": []interface{}{4, 4},
	"LLEN":   []interface{}{2, 2},
	// zset
	"ZADD":             []interface{}{4, -1},
	"ZCARD":            []interface{}{2, 2},
	"ZRANK":            []interface{}{3, 3},
	"ZREVRANK":         []interface{}{3, 3},
	"ZRANGE":           []interface{}{4, 5},
	"ZREVRANGE":        []interface{}{4, 5},
	"ZRANGEBYSCORE":    []interface{}{4, -1},
	"ZREVRANGEBYSCORE": []interface{}{4, -1},
	"ZREM":             []interface{}{3, -1},
	"ZREMRANGEBYRANK":  []interface{}{4, 4},
	"ZREMRANGEBYSCORE": []interface{}{4, 4},
	"ZINCRBY":          []interface{}{4, 4},
	"ZSCORE":           []interface{}{3, 3},
	// server
	"CLIENT": []interface{}{2, 2},
	"AOF":    []interface{}{2, 2},
}

func verifyCommand(cmd *Command) error {
	if cmd == nil || cmd.Len() == 0 {
		return BadCommandError
	}

	name := cmd.Name()
	rule, exist := cmdrules[name]
	if !exist {
		return nil
	}

	for i, count := 0, len(rule); i < count; i++ {
		switch i {
		case RI_MinCount:
			if val := rule[i].(int); val != -1 && cmd.Len() < val {
				return WrongArgumentCount
			}
		case RI_MaxCount:
			if val := rule[i].(int); val != -1 && cmd.Len() > val {
				return WrongArgumentCount
			}
		}
	}

	if cmd.Len() > 1 {
		key := cmd.StringAtIndex(1)
		if strings.ContainsAny(key, "#[] ") {
			return WrongCommandKey
		}
	}
	return nil
}
