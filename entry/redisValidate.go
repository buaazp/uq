package entry

import (
	"errors"
	"strings"
)

var (
	errBadCommand         = errors.New("bad command")
	errWrongArgumentCount = errors.New("wrong argument count")
	errWrongCommandKey    = errors.New("wrong command key")
)

const (
	riMinCount = iota
	riMaxCount // -1 for undefined
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

func verifyCommand(cmd *command) error {
	if cmd == nil || cmd.length() == 0 {
		return errBadCommand
	}

	name := cmd.name()
	rule, exist := cmdrules[name]
	if !exist {
		return nil
	}

	for i, count := 0, len(rule); i < count; i++ {
		switch i {
		case riMinCount:
			if val := rule[i].(int); val != -1 && cmd.length() < val {
				return errWrongArgumentCount
			}
		case riMaxCount:
			if val := rule[i].(int); val != -1 && cmd.length() > val {
				return errWrongArgumentCount
			}
		}
	}

	if cmd.length() > 1 {
		key := cmd.stringAtIndex(1)
		if strings.ContainsAny(key, "#[] ") {
			return errWrongCommandKey
		}
	}
	return nil
}
