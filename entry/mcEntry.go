package entry

import (
	"bufio"
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
	// listener     net.Listener
	messageQueue queue.MessageQueue
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

func NewMcEntry(host string, port int, messageQueue queue.MessageQueue) (*McEntry, error) {
	mc := new(McEntry)
	mc.host = host
	mc.port = port
	mc.messageQueue = messageQueue
	return mc, nil
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
	case "get", "gets", "stats":
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
	case "verbosity":

	default:
		return nil, NewError(
			ErrBadRequest,
			`unknow command: `+req.Cmd,
		)
	}
	return req, nil
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
		// log.Printf("unexpected error: %v", err)
		resp.status = "SERVER_ERROR"
		resp.msg = e.Error()
	}
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

	case "stats":
		key := req.Keys[0]
		resp.status = "STAT"
		qs, err := m.messageQueue.Stat(key)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}

		// for human reading
		resp.msg = qs.ToMcString()

		// for json format
		// data, err := qs.ToJson()
		// if err != nil {
		// 	writeErrorMc(resp, NewError(
		// 		ErrInternalError,
		// 		err.Error(),
		// 	))
		// 	return
		// }
		// resp.msg = string(data)

	case "add":
		key := req.Keys[0]
		recycle := string(req.Item.Body)

		// log.Printf("creating... %s %s", key, recycle)
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

func (m *McEntry) handlerConn(conn net.Conn) {
	// addr := conn.RemoteAddr().String()
	// log.Printf("handleClient: %s", addr)

	rbuf := bufio.NewReader(conn)
	wbuf := bufio.NewWriter(conn)

	for {
		req, err := m.Read(rbuf)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
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

	// log.Printf("conn %s closing...", addr)
	if err := conn.Close(); err != nil {
		// log.Printf("conn %s close error: %s", addr, err)
	}

	return
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
	// m.listener = l

	log.Printf("mc entrance serving at %s...", addr)
	for {
		conn, e := m.stopListener.Accept()
		// conn, e := m.listener.Accept()
		if e != nil {
			// log.Printf("Accept failed: %s\n", e)
			return e
		}

		go m.handlerConn(conn)
	}

	return nil
}

func (m *McEntry) Stop() {
	log.Printf("mc entry stoping...")
	m.stopListener.Stop()
	// m.listener.Close()
	m.messageQueue.Close()
	log.Printf("mc entry stoped.")
}
