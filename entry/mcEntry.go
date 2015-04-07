package entry

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

const (
	ERR_C_FORMAT   = "bad command line format"
	ERR_C_DLENGTH  = "bad data length"
	ERR_C_DCHUNK   = "bad data chunk"
	ERR_S_INTERNAL = "internal error"
	ERR_S_LLONG    = "output line too long"
	ERR_S_MEMORY   = "out of memory"
	ERR_S_DELETE   = "while deleting an item"
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
				resp.status = "ERROR"
				resp.msg = err.Error()
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
		return nil, err
	}
	if !strings.HasSuffix(s, "\r\n") {
		log.Println("has not suffix")
		return nil, errors.New(ERR_C_FORMAT)
	}
	parts := strings.Fields(s)
	if len(parts) < 1 {
		log.Println("cmd fiedld error")
		return nil, errors.New(ERR_C_FORMAT)
	}

	req := new(Request)
	req.Cmd = parts[0]
	switch req.Cmd {
	case "get", "gets":
		if len(parts) < 2 {
			log.Println("cmd parts error")
			return nil, errors.New(ERR_C_FORMAT)
		}
		req.Keys = parts[1:]

	case "set", "add":
		if len(parts) < 5 || len(parts) > 7 {
			log.Println("cmd parts error")
			return nil, errors.New(ERR_C_FORMAT)
		}
		req.Keys = parts[1:2]
		item := new(Item)
		item.Flag, err = strconv.Atoi(parts[2])
		if err != nil {
			log.Println("flag atoi failed: ", err)
			return nil, errors.New(ERR_C_FORMAT)
		}
		item.Exptime, err = strconv.Atoi(parts[3])
		if err != nil {
			log.Println("exptime atoi failed: ", err)
			return nil, errors.New(ERR_C_FORMAT)
		}
		length, err := strconv.Atoi(parts[4])
		if err != nil {
			log.Println("length atoi failed: ", err)
			return nil, errors.New(ERR_C_FORMAT)
		}
		if length > MaxBodyLength {
			log.Println("data length too large")
			return nil, errors.New(ERR_C_DLENGTH)
		}
		if len(parts) > 5 && parts[5] != "noreply" {
			log.Println("cmd parts error")
			return nil, errors.New(ERR_C_FORMAT)
		}
		req.NoReply = len(parts) > 5 && parts[5] == "noreply"

		item.Body = make([]byte, length)
		_, err = io.ReadFull(b, item.Body)
		if err != nil {
			log.Println("readfull failed: ", err)
			return nil, err
		}
		remain, _, err := b.ReadLine()
		if err != nil {
			log.Println("readline failed: ", err)
			return nil, err
		}
		if len(remain) != 0 {
			log.Println("bad data chunk", len(remain))
			return nil, errors.New(ERR_C_DCHUNK)
		}
		req.Item = item

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			log.Println("cmd parts error")
			return nil, errors.New(ERR_C_FORMAT)
		}
		req.Keys = parts[1:2]
		req.NoReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "quit", "version", "flush_all":
	case "replace", "cas", "append", "prepend":
	case "incr", "decr":
	case "stats":
	case "verbosity":

	default:
		log.Printf("unknow cmd: %s\n", req.Cmd)
		return nil, errors.New("unknow command: " + req.Cmd)
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
				resp.status = "CLIENT_ERROR"
				resp.msg = "bad key too long"
				return
			}
		}

		key := req.Keys[0]
		resp.status = "VALUE"
		id, data, err := m.messageQueue.Pop(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		if len(data) > 0 {
			itemMsg := new(Item)
			itemMsg.Body = data
			items := make(map[string]*Item)
			items[key] = itemMsg

			if len(req.Keys) > 1 {
				keyID := req.Keys[1]
				itemID := new(Item)
				itemID.Body = []byte(Acatui(key, "/", id))
				items[keyID] = itemID
			}

			resp.items = items
		}

	case "add":
		key := req.Keys[0]

		qr := new(queue.QueueRequest)
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			qr.TopicName = parts[0]
			qr.LineName = parts[1]
			if len(req.Item.Body) > 0 {
				data := string(req.Item.Body)
				recycle, err := time.ParseDuration(data)
				if err != nil {
					resp.status = "CLIENT_ERROR"
					resp.msg = err.Error()
					return
				}
				qr.Recycle = recycle
			}
		} else if len(parts) == 1 {
			qr.TopicName = parts[0]
		} else {
			resp.status = "CLIENT_ERROR"
			resp.msg = ERR_C_FORMAT
			return
		}

		log.Printf("creating... %v", qr)
		err = m.messageQueue.Create(qr)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		resp.status = "STORED"

	case "set":
		key := req.Keys[0]
		err = m.messageQueue.Push(key, req.Item.Body)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		resp.status = "STORED"

	case "delete":
		key := req.Keys[0]

		err = m.messageQueue.Confirm(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}
		resp.status = "DELETED"

	case "quit":
		resp = nil
		quit = true
		return

	default:
		// client error
		resp.status = "ERROR"
		resp.msg = ERR_C_FORMAT
	}
	return
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
