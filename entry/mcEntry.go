package entry

import (
	"bufio"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/utils"
)

// McEntry is the memcached entrance of uq
type McEntry struct {
	host         string
	port         int
	stopListener *utils.StopListener
	messageQueue queue.MessageQueue
}

type item struct {
	flag    int
	expTime int
	body    []byte
}

type request struct {
	cmd     string   // get, set, delete, quit, etc.
	keys    []string // key
	item    *item
	noReply bool
}

// NewMcEntry returns a new McEntry server
func NewMcEntry(host string, port int, messageQueue queue.MessageQueue) (*McEntry, error) {
	mc := new(McEntry)
	mc.host = host
	mc.port = port
	mc.messageQueue = messageQueue
	return mc, nil
}

func (m *McEntry) read(b *bufio.Reader) (*request, error) {
	s, err := b.ReadString('\n')
	if err != nil {
		log.Println("readstring failed: ", err)
		return nil, utils.NewError(
			utils.ErrBadRequest,
			err.Error(),
		)
	}
	if !strings.HasSuffix(s, "\r\n") {
		return nil, utils.NewError(
			utils.ErrBadRequest,
			`has not suffix \r\n`,
		)
	}
	parts := strings.Fields(s)
	if len(parts) < 1 {
		return nil, utils.NewError(
			utils.ErrBadRequest,
			`cmd fields error < 1`,
		)
	}

	req := new(request)
	req.cmd = parts[0]
	switch req.cmd {
	case "get", "gets", "stats":
		if len(parts) < 2 {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`cmd parts error: < 2`,
			)
		}
		req.keys = parts[1:]

	case "set", "add":
		if len(parts) < 5 || len(parts) > 7 {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`cmd parts error: < 5 or > 7`,
			)
		}
		req.keys = parts[1:2]
		item := new(item)
		item.flag, err = strconv.Atoi(parts[2])
		if err != nil {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`flag atoi failed: `+err.Error(),
			)
		}
		item.expTime, err = strconv.Atoi(parts[3])
		if err != nil {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`exptime atoi failed: `+err.Error(),
			)
		}
		length, err := strconv.Atoi(parts[4])
		if err != nil {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`length atoi failed: `+err.Error(),
			)
		}
		if length > MaxBodyLength {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`bad data length`,
			)
		}
		if len(parts) > 5 && parts[5] != "noreply" {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`cmd parts error: > 5 or part[5] != noreply`,
			)
		}
		req.noReply = len(parts) > 5 && parts[5] == "noreply"

		item.body = make([]byte, length)
		_, err = io.ReadFull(b, item.body)
		if err != nil {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`readfull failed: `+err.Error(),
			)
		}
		remain, _, err := b.ReadLine()
		if err != nil {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`readline failed: `+err.Error(),
			)
		}
		if len(remain) != 0 {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`bad data chunk`,
			)
		}
		req.item = item

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			return nil, utils.NewError(
				utils.ErrBadRequest,
				`cmd parts error: < 2 or > 4`,
			)
		}
		req.keys = parts[1:2]
		req.noReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "quit", "version", "flush_all":
	case "replace", "cas", "append", "prepend":
	case "incr", "decr":
	case "verbosity":

	default:
		return nil, utils.NewError(
			utils.ErrBadRequest,
			`unknow command: `+req.cmd,
		)
	}
	return req, nil
}

func writeErrorMc(resp *response, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *utils.Error:
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

func (m *McEntry) process(req *request) (resp *response, quit bool) {
	var err error
	resp = new(response)
	quit = false
	resp.noreply = req.noReply

	switch req.cmd {
	case "get", "gets":
		for _, k := range req.keys {
			if len(k) > MaxKeyLength {
				writeErrorMc(resp, utils.NewError(
					utils.ErrBadKey,
					`key is too long`,
				))
				return
			}
		}

		key := req.keys[0]
		resp.status = "VALUE"
		id, data, err := m.messageQueue.Pop(key)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}

		itemMsg := new(item)
		itemMsg.body = data
		items := make(map[string]*item)
		items[key] = itemMsg

		if len(req.keys) > 1 {
			keyID := req.keys[1]
			itemID := new(item)
			itemID.body = []byte(id)
			items[keyID] = itemID
		}

		resp.items = items

	case "stats":
		key := req.keys[0]
		resp.status = "STAT"
		qs, err := m.messageQueue.Stat(key)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}

		// for human reading
		resp.msg = qs.ToMcString()

		// for json format
		// data, err := qs.ToJSON()
		// if err != nil {
		// 	writeErrorMc(resp, NewError(
		// 		ErrInternalError,
		// 		err.Error(),
		// 	))
		// 	return
		// }
		// resp.msg = string(data)

	case "add":
		key := req.keys[0]
		recycle := string(req.item.body)

		// log.Printf("creating... %s %s", key, recycle)
		err = m.messageQueue.Create(key, recycle)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}
		resp.status = "STORED"

	case "set":
		key := req.keys[0]
		err = m.messageQueue.Push(key, req.item.body)
		if err != nil {
			writeErrorMc(resp, err)
			return
		}
		resp.status = "STORED"

	case "delete":
		key := req.keys[0]

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
		writeErrorMc(resp, utils.NewError(
			utils.ErrBadRequest,
			`unknow command: `+req.cmd,
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
		req, err := m.read(rbuf)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				break
			} else {
				resp := new(response)
				writeErrorMc(resp, err)
				resp.Write(wbuf)
				wbuf.Flush()
				continue
			}
		}

		resp, quit := m.process(req)
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

// ListenAndServe implements the ListenAndServe interface
func (m *McEntry) ListenAndServe() error {
	addr := utils.Addrcat(m.host, m.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := utils.NewStopListener(l)
	if err != nil {
		return err
	}
	m.stopListener = stopListener

	log.Printf("mc entrance serving at %s...", addr)
	for {
		conn, e := m.stopListener.Accept()
		if e != nil {
			// log.Printf("Accept failed: %s\n", e)
			return e
		}

		go m.handlerConn(conn)
	}
}

// Stop implements the Stop interface
func (m *McEntry) Stop() {
	log.Printf("mc entry stoping...")
	m.stopListener.Stop()
	m.messageQueue.Close()
	log.Printf("mc entry stoped.")
}
