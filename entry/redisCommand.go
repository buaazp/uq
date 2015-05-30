package entry

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/buaazp/uq/utils"
)

type command struct {
	args  [][]byte
	attrs map[string]interface{}
}

func newCommand(args ...[]byte) (cmd *command) {
	cmd = &command{
		args:  args,
		attrs: make(map[string]interface{}),
	}
	return
}

func (cmd *command) setAttribute(name string, v interface{}) {
	cmd.attrs[name] = v
}

func (cmd *command) getAttribute(name string) interface{} {
	return cmd.attrs[name]
}

func (cmd *command) name() string {
	return string(bytes.ToUpper(cmd.args[0]))
}

func (cmd *command) stringArgs() []string {
	strings := make([]string, len(cmd.args))
	for i, arg := range cmd.args {
		strings[i] = string(arg)
	}
	return strings
}

func (cmd *command) stringAtIndex(i int) string {
	if i >= cmd.length() {
		return ""
	}
	return string(cmd.args[i])
}

func (cmd *command) argAtIndex(i int) (arg []byte, err error) {
	if i >= cmd.length() {
		err = fmt.Errorf("out of range %d/%d", i, cmd.length())
		return
	}
	arg = cmd.args[i]
	return
}

func (cmd *command) intAtIndex(i int) (n int, err error) {
	if i >= cmd.length() {
		err = fmt.Errorf("out of range %d/%d", i, cmd.length())
		return
	}
	n, err = strconv.Atoi(string(cmd.args[i]))
	return
}

func (cmd *command) int64AtIndex(i int) (n int64, err error) {
	if i >= cmd.length() {
		err = fmt.Errorf("out of range %d/%d", i, cmd.length())
		return
	}
	n, err = strconv.ParseInt(string(cmd.args[i]), 10, 0)
	return
}

func (cmd *command) uint64AtIndex(i int) (n uint64, err error) {
	if i >= cmd.length() {
		err = fmt.Errorf("out of range %d/%d", i, cmd.length())
		return
	}
	n, err = strconv.ParseUint(string(cmd.args[i]), 10, 0)
	return
}

func (cmd *command) floatAtIndex(i int) (n float64, err error) {
	if i >= cmd.length() {
		err = fmt.Errorf("out of range %d/%d", i, cmd.length())
		return
	}
	n, err = strconv.ParseFloat(string(cmd.args[i]), 64)
	return
}

func (cmd *command) length() int {
	return len(cmd.args)
}

/*
*<number of arguments> CR LF
$<number of bytes of argument 1> CR LF
<argument data> CR LF
...
$<number of bytes of argument N> CR LF
<argument data> CR LF
*/
func (cmd *command) bytes() []byte {
	buf := bytes.Buffer{}
	buf.WriteByte('*')
	argCount := cmd.length()
	buf.WriteString(utils.ItoaQuick(argCount)) //<number of arguments>
	buf.WriteString(CRLF)
	for i := 0; i < argCount; i++ {
		buf.WriteByte('$')
		argSize := len(cmd.args[i])
		buf.WriteString(utils.ItoaQuick(argSize)) //<number of bytes of argument i>
		buf.WriteString(CRLF)
		buf.Write(cmd.args[i]) //<argument data>
		buf.WriteString(CRLF)
	}
	return buf.Bytes()
}

func parseCommand(buf *bytes.Buffer) (*command, error) {
	// Read ( *<number of arguments> CR LF )
	if c, err := buf.ReadByte(); c != '*' { // io.EOF
		return nil, err
	}
	// number of arguments
	line, err := buf.ReadBytes(LF)
	if err != nil {
		return nil, err
	}
	argCount, _ := strconv.Atoi(string(line[:len(line)-2]))
	args := make([][]byte, argCount)
	for i := 0; i < argCount; i++ {
		// Read ( $<number of bytes of argument 1> CR LF )
		if c, err := buf.ReadByte(); c != '$' {
			return nil, err
		}

		line, err := buf.ReadBytes(LF)
		if err != nil {
			return nil, err
		}
		argSize, _ := strconv.Atoi(string(line[:len(line)-2]))
		// Read ( <argument data> CR LF )
		args[i] = make([]byte, argSize)
		n, e2 := buf.Read(args[i])
		if n != argSize {
			return nil, errors.New("argSize too short")
		}
		if e2 != nil {
			return nil, e2
		}

		_, err = buf.ReadBytes(LF)
		if err != nil {
			return nil, err
		}
	}
	cmd := newCommand(args...)
	return cmd, nil
}

func (cmd *command) String() string {
	buf := bytes.Buffer{}
	for i, count := 0, cmd.length(); i < count; i++ {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.Write(cmd.args[i])
	}
	return buf.String()
}
