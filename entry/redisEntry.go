package entry

import (
	"fmt"
	"log"
	"time"

	. "github.com/buaazp/uq/entry/goredis"
	"github.com/buaazp/uq/queue"
	"github.com/latermoon/GoRedis/libs/stdlog"
)

// Command属性
const (
	C_SESSION = "session"
	C_ELAPSED = "elapsed"
)

type RedisEntry struct {
	host         string
	port         int
	redisServer  *RedisServer
	messageQueue queue.MessageQueue
}

func NewRedisEntry(host string, port int, messageQueue queue.MessageQueue) (*RedisEntry, error) {
	rs := new(RedisEntry)
	rs.host = host
	rs.port = port
	redisServer := new(RedisServer)
	redisServer.SetHandler(rs)
	rs.redisServer = redisServer
	rs.messageQueue = messageQueue
	return rs, nil
}

func (r *RedisEntry) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", r.host, r.port)
	return r.redisServer.Listen(addr)
}

// ServerHandler.SessionOpened()
func (r *RedisEntry) SessionOpened(session *Session) {
	log.Println("connection accepted from", session.RemoteAddr())
}

// ServerHandler.SessionClosed()
func (r *RedisEntry) SessionClosed(session *Session, err error) {
	log.Println("end connection", session.RemoteAddr(), err)
}

func (r *RedisEntry) ExceptionCaught(err error) {
	log.Printf("exception %s\n", err)
}

// ServerHandler.On()
// 由GoRedis协议层触发，通过反射调用OnGET/OnSET等方法
func (r *RedisEntry) On(session *Session, cmd *Command) (reply *Reply) {
	// invoke & time
	begin := time.Now()
	cmd.SetAttribute(C_SESSION, session)

	// varify command
	if err := verifyCommand(cmd); err != nil {
		stdlog.Printf("[%s] bad command %s\n", session.RemoteAddr(), cmd)
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

	if cmdName == "ADD" {
		reply = r.OnADD(cmd)
	} else if cmdName == "SET" {
		reply = r.OnSET(cmd)
	} else if cmdName == "GET" {
		reply = r.OnGET(cmd)
	} else if cmdName == "DEL" {
		reply = r.OnDEL(cmd)
	} else {
		reply = r.OnUndefined(session, cmd)
	}

	return
}

func (r *RedisEntry) OnUndefined(session *Session, cmd *Command) (reply *Reply) {
	return ErrorReply("NotSupported: " + cmd.String())
}

func (r *RedisEntry) Stop() {
	log.Printf("redis entry stoping...")
	r.redisServer.Stop()
	r.messageQueue.Close()
}
