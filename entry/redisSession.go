package entry

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/buaazp/uq/utils"
)

type session struct {
	net.Conn
	rw    *bufio.Reader
	attrs map[string]interface{}
}

func newSession(conn net.Conn) (s *session) {
	s = &session{
		Conn:  conn,
		attrs: make(map[string]interface{}),
	}
	s.rw = bufio.NewReader(s.Conn)
	return
}

func (s *session) setAttribute(name string, v interface{}) {
	s.attrs[name] = v
}

func (s *session) getAttribute(name string) interface{} {
	return s.attrs[name]
}

func (s *session) writeReply(rep *reply) (err error) {
	switch rep.rType {
	case replyTypeStatus:
		err = s.replyStatus(rep.value.(string))
	case replyTypeError:
		err = s.replyError(rep.value.(string))
	case replyTypeInteger:
		err = s.replyInteger(rep.value.(int))
	case replyTypeBulk:
		err = s.replyBulk(rep.value)
	case replyTypeMultiBulks:
		err = s.replyMultiBulks(rep.value.([]interface{}))
	default:
		err = errors.New("Illegal ReplyType: " + utils.ItoaQuick(int(rep.rType)))
	}
	return
}

func (s *session) writeCommand(cmd *command) (err error) {
	_, err = s.Write(cmd.bytes())
	return
}

// ReadReply reads the reply from the session
// In a Status Reply the first byte of the reply is "+"
// In an Error Reply the first byte of the reply is "-"
// In an Integer Reply the first byte of the reply is ":"
// In a Bulk Reply the first byte of the reply is "$"
// In a Multi Bulk Reply the first byte of the reply s "*"
func (s *session) readReply() (rep *reply, err error) {
	reader := s.rw
	var c byte
	if c, err = reader.ReadByte(); err != nil {
		return
	}

	rep = &reply{}
	switch c {
	case '+':
		rep.rType = replyTypeStatus
		rep.value, err = s.readString()
	case '-':
		rep.rType = replyTypeError
		rep.value, err = s.readString()
	case ':':
		rep.rType = replyTypeInteger
		rep.value, err = s.readInt()
	case '$':
		rep.rType = replyTypeBulk
		var bufsize int
		bufsize, err = s.readInt()
		if err != nil {
			break
		}
		buf := make([]byte, bufsize)
		_, err = io.ReadFull(s, buf)
		if err != nil {
			break
		}
		rep.value = buf
		s.skipBytes([]byte{CR, LF})
	case '*':
		rep.rType = replyTypeMultiBulks
		var argCount int
		argCount, err = s.readInt()
		if err != nil {
			break
		}
		if argCount == -1 {
			rep.value = nil // *-1
		} else {
			args := make([]interface{}, argCount)
			for i := 0; i < argCount; i++ {
				// TODO multi bulk 的类型 $和:
				err = s.skipByte('$')
				if err != nil {
					break
				}
				var argSize int
				argSize, err = s.readInt()
				if err != nil {
					return
				}
				if argSize == -1 {
					args[i] = nil
				} else {
					arg := make([]byte, argSize)
					_, err = io.ReadFull(s, arg)
					if err != nil {
						break
					}
					args[i] = arg
				}
				s.skipBytes([]byte{CR, LF})
			}
			rep.value = args
		}
	default:
		err = errors.New("Bad Reply Flag:" + string([]byte{c}))
	}
	return
}

/*
*<number of arguments> CR LF
$<number of bytes of argument 1> CR LF
<argument data> CR LF
...
$<number of bytes of argument N> CR LF
<argument data> CR LF
*/
func (s *session) readCommand() (cmd *command, err error) {
	// Read ( *<number of arguments> CR LF )
	err = s.skipByte('*')
	if err != nil { // io.EOF
		return
	}
	// number of arguments
	var argCount int
	if argCount, err = s.readInt(); err != nil {
		return
	}
	args := make([][]byte, argCount)
	for i := 0; i < argCount; i++ {
		// Read ( $<number of bytes of argument 1> CR LF )
		err = s.skipByte('$')
		if err != nil {
			return
		}

		var argSize int
		argSize, err = s.readInt()
		if err != nil {
			return
		}

		// Read ( <argument data> CR LF )
		args[i] = make([]byte, argSize)
		_, err = io.ReadFull(s, args[i])
		if err != nil {
			return
		}

		err = s.skipBytes([]byte{CR, LF})
		if err != nil {
			return
		}
	}
	cmd = newCommand(args...)
	return
}

// Status reply
func (s *session) replyStatus(status string) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString("+")
	buf.WriteString(status)
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Error reply
func (s *session) replyError(errmsg string) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString("-")
	buf.WriteString(errmsg)
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Integer reply
func (s *session) replyInteger(i int) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString(":")
	buf.WriteString(utils.ItoaQuick(i))
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Bulk Reply
func (s *session) replyBulk(bulk interface{}) (err error) {
	// NULL Bulk Reply
	isnil := bulk == nil
	if !isnil {
		b, ok := bulk.([]byte)
		isnil = ok && b == nil
	}
	if isnil {
		_, err = s.Write([]byte("$-1\r\n"))
		return
	}
	buf := bytes.Buffer{}
	buf.WriteString("$")
	switch bulk.(type) {
	case []byte:
		b := bulk.([]byte)
		buf.WriteString(utils.ItoaQuick(len(b)))
		buf.WriteString(CRLF)
		buf.Write(b)
	default:
		b := []byte(bulk.(string))
		buf.WriteString(utils.ItoaQuick(len(b)))
		buf.WriteString(CRLF)
		buf.Write(b)
	}
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Multi-bulk replies
func (s *session) replyMultiBulks(bulks []interface{}) (err error) {
	// Null Multi Bulk Reply
	if bulks == nil {
		_, err = s.Write([]byte("*-1\r\n"))
		return
	}
	bulkCount := len(bulks)
	// Empty Multi Bulk Reply
	if bulkCount == 0 {
		_, err = s.Write([]byte("*0\r\n"))
		return
	}
	buf := bytes.Buffer{}
	buf.WriteString("*")
	buf.WriteString(utils.ItoaQuick(bulkCount))
	buf.WriteString(CRLF)
	for i := 0; i < bulkCount; i++ {
		bulk := bulks[i]
		switch bulk.(type) {
		case string:
			buf.WriteString("$")
			b := []byte(bulk.(string))
			buf.WriteString(utils.ItoaQuick(len(b)))
			buf.WriteString(CRLF)
			buf.Write(b)
			buf.WriteString(CRLF)
		case []byte:
			b := bulk.([]byte)
			if b == nil {
				buf.WriteString("$-1")
				buf.WriteString(CRLF)
			} else {
				buf.WriteString("$")
				buf.WriteString(utils.ItoaQuick(len(b)))
				buf.WriteString(CRLF)
				buf.Write(b)
				buf.WriteString(CRLF)
			}
		case int:
			buf.WriteString(":")
			buf.WriteString(utils.ItoaQuick(bulk.(int)))
			buf.WriteString(CRLF)
		case uint64:
			buf.WriteString(":")
			buf.WriteString(strconv.FormatUint(bulk.(uint64), 10))
			buf.WriteString(CRLF)
		default:
			// nil element
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		}
	}
	// flush
	_, err = buf.WriteTo(s)
	return
}

// ====================================
// io
// ====================================
func (s *session) skipByte(c byte) (err error) {
	var tmp byte
	tmp, err = s.rw.ReadByte()
	if err != nil {
		return
	}
	if tmp != c {
		err = fmt.Errorf("Illegal Byte [%d] != [%d]", tmp, c)
	}
	return
}

func (s *session) skipBytes(bs []byte) (err error) {
	for _, c := range bs {
		err = s.skipByte(c)
		if err != nil {
			break
		}
	}
	return
}

func (s *session) readLine() (line []byte, err error) {
	line, err = s.rw.ReadSlice(LF)
	if err == bufio.ErrBufferFull {
		return nil, errors.New("line too long")
	}
	if err != nil {
		return
	}
	i := len(line) - 2
	if i < 0 || line[i] != CR {
		err = errors.New("bad line terminator:" + string(line))
	}
	return line[:i], nil
}

func (s *session) readString() (str string, err error) {
	var line []byte
	if line, err = s.readLine(); err != nil {
		return
	}
	str = string(line)
	return
}

func (s *session) readInt() (i int, err error) {
	var line string
	if line, err = s.readString(); err != nil {
		return
	}
	i, err = strconv.Atoi(line)
	return
}

func (s *session) readInt64() (i int64, err error) {
	var line string
	if line, err = s.readString(); err != nil {
		return
	}
	i, err = strconv.ParseInt(line, 10, 64)
	return
}

func (s *session) Read(p []byte) (n int, err error) {
	return s.rw.Read(p)
}

func (s *session) readByte() (c byte, err error) {
	return s.rw.ReadByte()
}

func (s *session) peekByte() (c byte, err error) {
	if b, e := s.rw.Peek(1); e == nil {
		c = b[0]
	}
	return
}

func (s *session) readRDB(w io.Writer) (err error) {
	// Read ( $<number of bytes of RDB> CR LF )
	if err = s.skipByte('$'); err != nil {
		return
	}

	var rdbSize int64
	if rdbSize, err = s.readInt64(); err != nil {
		return
	}

	var c byte
	for i := int64(0); i < rdbSize; i++ {
		c, err = s.rw.ReadByte()
		if err != nil {
			return
		}
		w.Write([]byte{c})
	}
	return
}

func (s *session) String() string {
	return fmt.Sprintf("<session:%s>", s.RemoteAddr())
}
