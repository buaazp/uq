package entry

import (
	"log"
	"net"
	"time"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/utils"
)

const (
	// CR is the \r
	CR = '\r'
	// LF is the \n
	LF = '\n'
	// CRLF is the \r\n
	CRLF     = "\r\n"
	cSession = "session"
	cElapsed = "elapsed"
)

// RedisEntry is the redis entrance of uq
type RedisEntry struct {
	host         string
	port         int
	stopListener *utils.StopListener
	messageQueue queue.MessageQueue
}

// NewRedisEntry returns a new RedisEntry
func NewRedisEntry(host string, port int, messageQueue queue.MessageQueue) (*RedisEntry, error) {
	rs := new(RedisEntry)
	rs.host = host
	rs.port = port
	rs.messageQueue = messageQueue
	return rs, nil
}

func (r *RedisEntry) onUndefined(ss *session, cmd *command) (rep *reply) {
	return errorReply(utils.NewError(
		utils.ErrBadRequest,
		"command not supported: "+cmd.String(),
	))
}

func (r *RedisEntry) commandHandler(ss *session, cmd *command) (rep *reply) {
	cmdName := cmd.name()

	if cmdName == "ADD" || cmdName == "QADD" {
		rep = r.onQadd(cmd)
	} else if cmdName == "SET" || cmdName == "QPUSH" {
		rep = r.onQpush(cmd)
	} else if cmdName == "MSET" || cmdName == "QMPUSH" {
		rep = r.onQmpush(cmd)
	} else if cmdName == "GET" || cmdName == "QPOP" {
		rep = r.onQpop(cmd)
	} else if cmdName == "MGET" || cmdName == "QMPOP" {
		rep = r.onQmpop(cmd)
	} else if cmdName == "DEL" || cmdName == "QDEL" {
		rep = r.onQdel(cmd)
	} else if cmdName == "MDEL" || cmdName == "QMDEL" {
		rep = r.onQmdel(cmd)
	} else if cmdName == "EMPTY" || cmdName == "QEMPTY" {
		rep = r.onQempty(cmd)
	} else if cmdName == "INFO" || cmdName == "QINFO" {
		rep = r.onInfo(cmd)
	} else {
		rep = r.onUndefined(ss, cmd)
	}

	return
}

func (r *RedisEntry) process(ss *session, cmd *command) (rep *reply) {
	// invoke & time
	begin := time.Now()
	cmd.setAttribute(cSession, ss)

	// varify command
	if err := verifyCommand(cmd); err != nil {
		// log.Printf("[%s] bad command %s\n", ss.RemoteAddr(), cmd)
		return errorReply(utils.NewError(
			utils.ErrBadRequest,
			err.Error(),
		))
	}

	// invoke
	rep = r.commandHandler(ss, cmd)

	elapsed := time.Now().Sub(begin)
	cmd.setAttribute(cElapsed, elapsed)

	return
}

func (r *RedisEntry) handlerConn(ss *session) {
	// addr := ss.RemoteAddr().String()
	// log.Printf("handleClient: %s", addr)

	for {
		// var cmd *command
		cmd, err := ss.readCommand()
		// 1) io.EOF
		// 2) read tcp 127.0.0.1:51863: connection reset by peer
		if err != nil {
			log.Printf("session read command error: %s", err)
			break
		}
		// log.Printf("cmd: %v", cmd)
		rep := r.process(ss, cmd)
		if rep != nil {
			err = ss.writeReply(rep)
			if err != nil {
				break
			}
		}
	}

	// log.Printf("session %s closing...", addr)
	if err := ss.Close(); err != nil {
		// log.Printf("session %s close error: %s", addr, err)
	}

	return
}

// ListenAndServe implements the ListenAndServe interface
func (r *RedisEntry) ListenAndServe() error {
	addr := utils.Addrcat(r.host, r.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := utils.NewStopListener(l)
	if err != nil {
		return err
	}
	r.stopListener = stopListener

	log.Printf("redis entrance serving at %s...", addr)
	for {
		conn, err := r.stopListener.Accept()
		if err != nil {
			// log.Printf("Accept failed: %s\n", err)
			return err
		}
		go r.handlerConn(newSession(conn))
	}
}

// Stop implements the Stop interface
func (r *RedisEntry) Stop() {
	log.Printf("redis entry stoping...")
	r.stopListener.Stop()
	r.messageQueue.Close()
}
