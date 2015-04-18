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
	"ADD":    []interface{}{2, 3},
	"QADD":   []interface{}{2, 3},
	"SET":    []interface{}{3, 3},
	"QPUSH":  []interface{}{3, 3},
	"MSET":   []interface{}{3, -1},
	"QMPUSH": []interface{}{3, -1},
	"GET":    []interface{}{2, 2},
	"QPOP":   []interface{}{2, 2},
	"MGET":   []interface{}{3, -1},
	"QMPOP":  []interface{}{3, -1},
	"DEL":    []interface{}{2, 2},
	"QDEL":   []interface{}{2, 2},
	"MDEL":   []interface{}{2, -1},
	"QMDEL":  []interface{}{2, -1},
	"EMPTY":  []interface{}{2, 2},
	"QEMPTY": []interface{}{2, 2},
	"INFO":   []interface{}{2, 2},
	"QINFO":  []interface{}{2, 2},
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
