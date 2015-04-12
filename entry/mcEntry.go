package entry

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

type McEntry struct {
	host         string
	port         int
	stopListener *StopListener
	messageQueue queue.MessageQueue
}

func NewMcEntry(host string, port int, messageQueue queue.MessageQueue) (*McEntry, error) {
	mc := new(McEntry)
	mc.host = host
	mc.port = port
	mc.messageQueue = messageQueue
	return mc, nil
}

func (m *McEntry) ListenAndServe() error {
	addr := Addrcat(m.host, m.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	m.stopListener = stopListener

	log.Printf("mc entrance serving at %s...", addr)
	for {
		conn, e := m.stopListener.Accept()
		if e != nil {
			log.Printf("Accept failed: %s\n", e)
			return e
		}

		go m.handlerConn(conn)
	}

	return nil
}

func (m *McEntry) handlerConn(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	log.Printf("handleClient: %s", addr)

	rbuf := bufio.NewReader(conn)
	wbuf := bufio.NewWriter(conn)

	for {
		req, err := m.Read(rbuf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				resp := new(Response)
				writeErrorMc(resp, err)
				resp.Write(wbuf)
				wbuf.Flush()
				continue
			}
		}

		resp, quit := m.Process(req)
		if quit {
			break
		}
		if resp == nil {
			continue
		}
		if !resp.noreply {
			resp.Write(wbuf)
			wbuf.Flush()
			continue
		}
	}

	log.Printf("conn %s closing...", addr)
	if err := conn.Close(); err != nil {
		log.Printf("conn %s close error: %s", addr, err)
	}

	return
}

type Item struct {
	Flag    int
	Exptime int
	Cas     int
	Body    []byte
}

type Request struct {
	Cmd     string   // get, set, delete, quit, etc.
	Keys    []string // key
	Item    *Item
	NoReply bool
}

func (m *McEntry) Read(b *bufio.Reader) (*Request, error) {
	s, err := b.ReadString('\n')
	if err != nil {
		log.Println("readstring failed: ", err)
		return nil, NewError(
			ErrBadRequest,
			err.Error(),
		)
	}
	if !strings.HasSuffix(s, "\r\n") {
		return nil, NewError(
			ErrBadRequest,
			`has not suffix \r\n`,
		)
	}
	parts := strings.Fields(s)
	if len(parts) < 1 {
		return nil, NewError(
			ErrBadRequest,
			`cmd fields error < 1`,
		)
	}

	req := new(Request)
	req.Cmd = parts[0]
	switch req.Cmd {
	case "get", "gets":
		if len(parts) < 2 {
			return nil, NewError(
				ErrBadRequest,
				`cmd parts error: < 2`,
			)
		}
		req.Keys = parts[1:]

	case "set", "add":
		if len(parts) < 5 || len(parts) > 7 {
			return nil, NewError(
				ErrBadRequest,
				`cmd parts error: < 5 or > 7`,
			)
		}
		req.Keys = parts[1:2]
		item := new(Item)
		item.Flag, err = strconv.Atoi(parts[2])
		if err != nil {
			return nil, NewError(
				ErrBadRequest,
				`flag atoi failed: `+err.Error(),
			)
		}
		item.Exptime, err = strconv.Atoi(parts[3])
		if err != nil {
			return nil, NewError(
				ErrBadRequest,
				`exptime atoi failed: `+err.Error(),
			)
		}
		length, err := strconv.Atoi(parts[4])
		if err != nil {
			return nil, NewError(
				ErrBadRequest,
				`length atoi failed: `+err.Error(),
			)
		}
		if length > MaxBodyLength {
			return nil, NewError(
				ErrBadRequest,
				`bad data length`,
			)
		}
		if len(parts) > 5 && parts[5] != "noreply" {
			return nil, NewError(
				ErrBadRequest,
				`cmd parts error: > 5 or part[5] != noreply`,
			)
		}
		req.NoReply = len(parts) > 5 && parts[5] == "noreply"

		item.Body = make([]byte, length)
		_, err = io.ReadFull(b, item.Body)
		if err != nil {
			return nil, NewError(
				ErrBadRequest,
				`readfull failed: `+err.Error(),
			)
		}
		remain, _, err := b.ReadLine()
		if err != nil {
			return nil, NewError(
				ErrBadRequest,
				`readline failed: `+err.Error(),
			)
		}
		if len(remain) != 0 {
			return nil, NewError(
				ErrBadRequest,
				`bad data chunk`,
			)
		}
		req.Item = item

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			return nil, NewError(
				ErrBadRequest,
				`cmd parts error: < 2 or > 4`,
			)
		}
		req.Keys = parts[1:2]
		req.NoReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "quit", "version", "flush_all":
	case "replace", "cas", "append", "prepend":
	case "incr", "decr":
	case "stats":
	case "verbosity":

	default:
		return nil, NewError(
			ErrBadRequest,
			`unknow command: `+req.Cmd,
		)
	}
	return req, nil
}

type Response struct {
	status  string
	msg     string
	noreply bool
	items   map[string]*Item
}

func (m *McEntry) Process(req *Request) (resp *Response, quit bool) {
	var err error
	resp = new(Response)
	quit = false
	resp.noreply = req.NoReply

	switch req.Cmd {
	case "get", "gets":
		for _, k := range req.Keys {
			if len(k) > MaxKeyLength {
				writeErrorMc(resp, NewError(
					ErrBadKey,
					`key is too long`,
				))
				return
			}
		}

		key := req.Keys[0]
		resp.status = "VALUE"
		id, data, err := m.messageQueue.Pop(key)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}

		itemMsg := new(Item)
		itemMsg.Body = data
		items := make(map[string]*Item)
		items[key] = itemMsg

		if len(req.Keys) > 1 {
			keyID := req.Keys[1]
			itemID := new(Item)
			itemID.Body = []byte(id)
			items[keyID] = itemID
		}

		resp.items = items

	case "add":
		key := req.Keys[0]
		recycle := string(req.Item.Body)

		log.Printf("creating... %s %s", key, recycle)
		err = m.messageQueue.Create(key, recycle)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}
		resp.status = "STORED"

	case "set":
		key := req.Keys[0]
		err = m.messageQueue.Push(key, req.Item.Body)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}
		resp.status = "STORED"

	case "delete":
		key := req.Keys[0]

		err = m.messageQueue.Confirm(key)
		if err != nil {
			writeErrorMc(resp, err)
			break
		}
		resp.status = "DELETED"

	case "quit":
		resp = nil
		quit = true
		return

	default:
		// client error
		writeErrorMc(resp, NewError(
			ErrBadRequest,
			`unknow command: `+req.Cmd,
		))
	}
	return
}

func writeErrorMc(resp *Response, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *Error:
		if e.ErrorCode >= 500 {
			resp.status = "SERVER_ERROR"
		} else {
			resp.status = "CLIENT_ERROR"
		}
		resp.msg = e.Error()
	default:
		log.Printf("unexpected error: %v", err)
		resp.status = "SERVER_ERROR"
		resp.msg = e.Error()
	}
}

func (resp *Response) Write(w io.Writer) error {
	if resp.noreply {
		return nil
	}

	switch resp.status {
	case "VALUE":
		if resp.items != nil {
			for key, item := range resp.items {
				fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.Flag, len(item.Body))
				if e := WriteFull(w, item.Body); e != nil {
					return e
				}
				WriteFull(w, []byte("\r\n"))
			}
		}
		io.WriteString(w, "END\r\n")

	case "STAT":
		io.WriteString(w, resp.msg)
		io.WriteString(w, "END\r\n")

	default:
		io.WriteString(w, resp.status)
		if resp.msg != "" {
			io.WriteString(w, " "+resp.msg)
		}
		io.WriteString(w, "\r\n")
	}
	return nil
}

func WriteFull(w io.Writer, buf []byte) error {
	n, e := w.Write(buf)
	for e != nil && n > 0 {
		buf = buf[n:]
		n, e = w.Write(buf)
	}
	return e
}

func (m *McEntry) Stop() {
	log.Printf("mc entry stoping...")
	m.stopListener.Stop()
	m.messageQueue.Close()
}
