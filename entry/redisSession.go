package entry

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

type Session struct {
	net.Conn
	rw    *bufio.Reader
	attrs map[string]interface{}
}

func NewSession(conn net.Conn) (s *Session) {
	s = &Session{
		Conn:  conn,
		attrs: make(map[string]interface{}),
	}
	s.rw = bufio.NewReader(s.Conn)
	return
}

func (s *Session) SetAttribute(name string, v interface{}) {
	s.attrs[name] = v
}

func (s *Session) GetAttribute(name string) interface{} {
	return s.attrs[name]
}

func (s *Session) WriteReply(reply *Reply) (err error) {
	switch reply.Type {
	case ReplyTypeStatus:
		err = s.replyStatus(reply.Value.(string))
	case ReplyTypeError:
		err = s.replyError(reply.Value.(string))
	case ReplyTypeInteger:
		err = s.replyInteger(reply.Value.(int))
	case ReplyTypeBulk:
		err = s.replyBulk(reply.Value)
	case ReplyTypeMultiBulks:
		err = s.replyMultiBulks(reply.Value.([]interface{}))
	default:
		err = errors.New("Illegal ReplyType: " + itoa(int(reply.Type)))
	}
	return
}

func (s *Session) WriteCommand(cmd *Command) (err error) {
	_, err = s.Write(cmd.Bytes())
	return
}

/*
In a Status Reply the first byte of the reply is "+"
In an Error Reply the first byte of the reply is "-"
In an Integer Reply the first byte of the reply is ":"
In a Bulk Reply the first byte of the reply is "$"
In a Multi Bulk Reply the first byte of the reply s "*"
*/
func (s *Session) ReadReply() (reply *Reply, err error) {
	reader := s.rw
	var c byte
	if c, err = reader.ReadByte(); err != nil {
		return
	}

	reply = &Reply{}
	switch c {
	case '+':
		reply.Type = ReplyTypeStatus
		reply.Value, err = s.readString()
	case '-':
		reply.Type = ReplyTypeError
		reply.Value, err = s.readString()
	case ':':
		reply.Type = ReplyTypeInteger
		reply.Value, err = s.readInt()
	case '$':
		reply.Type = ReplyTypeBulk
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
		reply.Value = buf
		s.skipBytes([]byte{CR, LF})
	case '*':
		reply.Type = ReplyTypeMultiBulks
		var argCount int
		argCount, err = s.readInt()
		if err != nil {
			break
		}
		if argCount == -1 {
			reply.Value = nil // *-1
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
			reply.Value = args
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
func (s *Session) ReadCommand() (cmd *Command, err error) {
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
	cmd = NewCommand(args...)
	return
}

// Status reply
func (s *Session) replyStatus(status string) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString("+")
	buf.WriteString(status)
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Error reply
func (s *Session) replyError(errmsg string) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString("-")
	buf.WriteString(errmsg)
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Integer reply
func (s *Session) replyInteger(i int) (err error) {
	buf := bytes.Buffer{}
	buf.WriteString(":")
	buf.WriteString(itoa(i))
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Bulk Reply
func (s *Session) replyBulk(bulk interface{}) (err error) {
	// NULL Bulk Reply
	isnil := bulk == nil
	if !isnil {
		// []byte 需要类型转换后才能判断
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
		buf.WriteString(itoa(len(b)))
		buf.WriteString(CRLF)
		buf.Write(b)
	default:
		b := []byte(bulk.(string))
		buf.WriteString(itoa(len(b)))
		buf.WriteString(CRLF)
		buf.Write(b)
	}
	buf.WriteString(CRLF)
	_, err = buf.WriteTo(s)
	return
}

// Multi-bulk replies
func (s *Session) replyMultiBulks(bulks []interface{}) (err error) {
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
	buf.WriteString(itoa(bulkCount))
	buf.WriteString(CRLF)
	for i := 0; i < bulkCount; i++ {
		bulk := bulks[i]
		switch bulk.(type) {
		case string:
			buf.WriteString("$")
			b := []byte(bulk.(string))
			buf.WriteString(itoa(len(b)))
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
				buf.WriteString(itoa(len(b)))
				buf.WriteString(CRLF)
				buf.Write(b)
				buf.WriteString(CRLF)
			}
		case int:
			buf.WriteString(":")
			buf.WriteString(itoa(bulk.(int)))
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
func (s *Session) skipByte(c byte) (err error) {
	var tmp byte
	tmp, err = s.rw.ReadByte()
	if err != nil {
		return
	}
	if tmp != c {
		err = errors.New(fmt.Sprintf("Illegal Byte [%d] != [%d]", tmp, c))
	}
	return
}

func (s *Session) skipBytes(bs []byte) (err error) {
	for _, c := range bs {
		err = s.skipByte(c)
		if err != nil {
			break
		}
	}
	return
}

func (s *Session) readLine() (line []byte, err error) {
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

func (s *Session) readString() (str string, err error) {
	var line []byte
	if line, err = s.readLine(); err != nil {
		return
	}
	str = string(line)
	return
}

func (s *Session) readInt() (i int, err error) {
	var line string
	if line, err = s.readString(); err != nil {
		return
	}
	i, err = strconv.Atoi(line)
	return
}

func (s *Session) readInt64() (i int64, err error) {
	var line string
	if line, err = s.readString(); err != nil {
		return
	}
	i, err = strconv.ParseInt(line, 10, 64)
	return
}

func (s *Session) ReadInt64() (i int64, err error) {
	return s.readInt64()
}

func (s *Session) Read(p []byte) (n int, err error) {
	return s.rw.Read(p)
}

func (s *Session) ReadByte() (c byte, err error) {
	return s.rw.ReadByte()
}

func (s *Session) PeekByte() (c byte, err error) {
	if b, e := s.rw.Peek(1); e == nil {
		c = b[0]
	}
	return
}

func (s *Session) ReadRDB(w io.Writer) (err error) {
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

func (s *Session) String() string {
	return fmt.Sprintf("<Session:%s>", s.RemoteAddr())
}
