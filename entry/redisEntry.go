package entry

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/buaazp/uq/queue"
)

const (
	CR        = '\r'
	LF        = '\n'
	CRLF      = "\r\n"
	C_SESSION = "session"
	C_ELAPSED = "elapsed"
)

type RedisEntry struct {
	host         string
	port         int
	stopListener *StopListener
	messageQueue queue.MessageQueue
}

func NewRedisEntry(host string, port int, messageQueue queue.MessageQueue) (*RedisEntry, error) {
	rs := new(RedisEntry)
	rs.host = host
	rs.port = port
	rs.messageQueue = messageQueue
	return rs, nil
}

func (r *RedisEntry) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", r.host, r.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	r.stopListener = stopListener

	log.Printf("redis entrance serving at %s...", addr)
	for {
		conn, err := r.stopListener.Accept()
		if err != nil {
			log.Printf("Accept failed: %s\n", err)
			return err
		}
		go r.handlerConn(NewSession(conn))
	}

	return nil
}

func (r *RedisEntry) handlerConn(session *Session) {
	var err error
	addr := session.RemoteAddr().String()
	log.Printf("handleClient: %s", addr)

	for {
		var cmd *Command
		cmd, err = session.ReadCommand()
		// 1) io.EOF
		// 2) read tcp 127.0.0.1:51863: connection reset by peer
		if err != nil {
			log.Printf("session read command error: %s", err)
			break
		}
		reply := r.Process(session, cmd)
		if reply != nil {
			err = session.WriteReply(reply)
			if err != nil {
				break
			}
		}
	}

	log.Printf("session %s closing...", addr)
	if err := session.Close(); err != nil {
		log.Printf("session %s close error: %s", addr, err)
	}

	return
}

func (r *RedisEntry) Process(session *Session, cmd *Command) (reply *Reply) {
	// invoke & time
	begin := time.Now()
	cmd.SetAttribute(C_SESSION, session)

	// varify command
	if err := verifyCommand(cmd); err != nil {
		log.Printf("[%s] bad command %s\n", session.RemoteAddr(), cmd)
		return ErrorReply(err)
	}

	// invoke
	reply = r.commandHandler(session, cmd)

	elapsed := time.Now().Sub(begin)
	cmd.SetAttribute(C_ELAPSED, elapsed)

	return
}

func (r *RedisEntry) commandHandler(session *Session, cmd *Command) (reply *Reply) {
	cmdName := cmd.Name()

	if cmdName == "QADD" {
		reply = r.OnQadd(cmd)
	} else if cmdName == "QPUSH" {
		reply = r.OnQpush(cmd)
	} else if cmdName == "QMPUSH" {
		reply = r.OnQmpush(cmd)
	} else if cmdName == "QPOP" {
		reply = r.OnQpop(cmd)
	} else if cmdName == "QMPOP" {
		reply = r.OnQmpop(cmd)
	} else if cmdName == "QDEL" {
		reply = r.OnQdel(cmd)
	} else if cmdName == "QMDEL" {
		reply = r.OnQmdel(cmd)
	} else {
		reply = r.OnUndefined(session, cmd)
	}

	return
}

func (r *RedisEntry) OnUndefined(session *Session, cmd *Command) (reply *Reply) {
	return ErrorReply("uq NotSupported: " + cmd.String())
}

func (r *RedisEntry) Stop() {
	log.Printf("redis entry stoping...")
	r.stopListener.Stop()
	r.messageQueue.Close()
}
