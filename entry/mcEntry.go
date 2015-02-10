package entry

import (
	"bufio"
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
	addr := fmt.Sprintf("%s:%d", m.host, m.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	m.stopListener = stopListener

	log.Print("start serving at ", addr, "...\n")
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
		req := new(Request)
		resp, quit := m.Read(rbuf, req)
		if quit {
			break
		}
		if resp != nil {
			resp.Write(wbuf)
			wbuf.Flush()
			continue
		}

		resp, quit = m.Process(req)
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

	log.Printf("handler closing...")
	if err := conn.Close(); err != nil {
		log.Printf("Close error: %s", err)
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
	Cmd     string // get, set, delete, quit, etc.
	Key     string // key
	Item    *Item
	NoReply bool
}

func (m *McEntry) Read(b *bufio.Reader, req *Request) (resp *Response, quit bool) {
	resp = new(Response)
	resp.status = "ERROR"
	quit = false
	s, err := b.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			quit = true
		}
		log.Println("readstring failed: ", err)
		return
	}
	if !strings.HasSuffix(s, "\r\n") {
		log.Println("has not suffix")
		return
	}
	parts := strings.Fields(s)
	if len(parts) < 1 {
		log.Println("cmd fiedld error")
		return
	}

	req.Cmd = parts[0]
	resp.status = "CLIENT_ERROR"
	switch req.Cmd {
	case "get", "gets":
		if len(parts) < 2 {
			log.Println("cmd parts error")
			resp.msg = ERR_C_FORMAT
			return
		}
		req.Key = parts[1]

	case "set", "add":
		if len(parts) < 5 || len(parts) > 7 {
			log.Println("cmd parts error")
			resp.msg = ERR_C_FORMAT
			return
		}
		req.Key = parts[1]
		req.Item = new(Item)
		item := req.Item
		item.Flag, err = strconv.Atoi(parts[2])
		if err != nil {
			log.Println("flag atoi failed: ", err)
			resp.msg = ERR_C_FORMAT
			return
		}
		// item.Exptime, err = strconv.Atoi(parts[3])
		// if err != nil {
		// 	log.Println("exptime atoi failed: ", err)
		// 	resp.msg = ERR_C_FORMAT
		// 	return
		// }
		length, err := strconv.Atoi(parts[4])
		if err != nil {
			log.Println("length atoi failed: ", err)
			resp.msg = ERR_C_FORMAT
			return
		}
		if length > MaxBodyLength {
			log.Println("data length too large")
			resp.msg = ERR_C_DLENGTH
			return
		}
		// if req.Cmd == "cas" {
		// 	if len(parts) < 6 {
		// 		log.Println("cmd parts error")
		// 		resp.msg = ERR_C_FORMAT
		// 		return
		// 	}
		// 	item.Cas, err = strconv.Atoi(parts[5])
		// 	if err != nil {
		// 		log.Println("cas atoi failed: ", err)
		// 		resp.msg = ERR_C_FORMAT
		// 		return
		// 	}
		// 	if len(parts) > 6 && parts[6] != "noreply" {
		// 		log.Println("cmd parts error")
		// 		resp.msg = ERR_C_FORMAT
		// 		return
		// 	}
		// 	req.NoReply = len(parts) > 6 && parts[6] == "noreply"
		// } else {
		if len(parts) > 5 && parts[5] != "noreply" {
			log.Println("cmd parts error")
			resp.msg = ERR_C_FORMAT
			return
		}
		req.NoReply = len(parts) > 5 && parts[5] == "noreply"
		// }

		item.Body = make([]byte, length)
		_, err = io.ReadFull(b, item.Body)
		if err != nil {
			if err == io.EOF {
				quit = true
			}
			log.Println("readfull failed: ", err)
			resp.msg = ERR_C_DCHUNK
			return
		}
		remain, _, err := b.ReadLine()
		if err != nil {
			if err == io.EOF {
				quit = true
			}
			log.Println("readline failed: ", err)
			resp.msg = ERR_C_DCHUNK
			return
		}
		if len(remain) != 0 {
			log.Println("bad data chunk", len(remain))
			resp.msg = ERR_C_DCHUNK
			return
		}

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			log.Println("cmd parts error")
			resp.msg = ERR_C_FORMAT
			return
		}
		req.Key = parts[1]
		req.NoReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "quit", "version", "flush_all":

	default:
		log.Printf("unknow cmd: %s\n", req.Cmd)
		resp.msg = "unknow command"
		return
	}
	return nil, false
}

type Response struct {
	status  string
	msg     string
	noreply bool
	key     string
	item    *Item
}

func (m *McEntry) Process(req *Request) (resp *Response, quit bool) {
	var err error
	resp = new(Response)
	quit = false
	resp.noreply = req.NoReply

	switch req.Cmd {
	case "get", "gets":
		key := req.Key
		if len(key) > MaxKeyLength {
			resp.status = "CLIENT_ERROR"
			resp.msg = "bad key too long"
			return
		}

		resp.status = "VALUE"
		id, data, err := m.messageQueue.Pop(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		if len(data) > 0 {
			// log.Printf("key: %s id: %v data: %v", req.Key, id, string(data))
			item := new(Item)
			item.Body = data
			resp.key = Acati(req.Key, "/", id)
			resp.item = item
		}

	case "add":
		key := req.Key

		cr := new(queue.CreateRequest)
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			cr.TopicName = parts[0]
			cr.LineName = parts[1]
			if len(req.Item.Body) > 0 {
				data := string(req.Item.Body)
				recycle, err := time.ParseDuration(data)
				if err != nil {
					resp.status = "CLIENT_ERROR"
					resp.msg = err.Error()
					return
				}
				cr.Recycle = recycle
			}
		} else if len(parts) == 1 {
			cr.TopicName = parts[0]
		} else {
			resp.status = "CLIENT_ERROR"
			resp.msg = ERR_C_FORMAT
			return
		}

		log.Printf("creating... %v", cr)
		err = m.messageQueue.Create(cr)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		resp.status = "STORED"

	case "set":
		key := req.Key
		err = m.messageQueue.Push(key, req.Item.Body)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}
		resp.status = "STORED"

	case "delete":
		key := req.Key

		cr := new(queue.ConfirmRequest)
		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			resp.status = "CLIENT_ERROR"
			resp.msg = ERR_C_FORMAT
			return
		} else {
			cr.TopicName = parts[0]
			cr.LineName = parts[1]
			id, err := strconv.ParseUint(parts[2], 10, 0)
			if err != nil {
				resp.status = "CLIENT_ERROR"
				resp.msg = ERR_C_FORMAT + err.Error()
				return
			}
			cr.ID = id
		}

		err = m.messageQueue.Confirm(cr)
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
	}
	return
}

func (resp *Response) Write(w io.Writer) error {
	if resp.noreply {
		return nil
	}

	switch resp.status {
	case "VALUE":
		if resp.item != nil {
			item := resp.item
			fmt.Fprintf(w, "VALUE %s %d %d\r\n", resp.key, item.Flag, len(item.Body))
			if e := WriteFull(w, item.Body); e != nil {
				return e
			}
			WriteFull(w, []byte("\r\n"))
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
