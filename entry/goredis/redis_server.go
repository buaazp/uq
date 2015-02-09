// Copyright 2013 Latermoon. All rights reserved.

// 使用Go实现RedisServer，并提供Redis协议读写所需要的各种方法
package goredis

import (
	"errors"
	"fmt"
	"log"
	"net"

	. "github.com/buaazp/uq/utils"
)

const (
	CR   = '\r'
	LF   = '\n'
	CRLF = "\r\n"
)

// 处理接收到的连接和数据
type ServerHandler interface {
	SessionOpened(session *Session)
	SessionClosed(session *Session, err error)
	On(session *Session, cmd *Command) (reply *Reply)
	ExceptionCaught(err error)
}

// ==============================
// RedisServer只实现最基本的Redis协议
// 提供On接口处理传入的各种指令，使用session返回数据
// ==============================
type RedisServer struct {
	// 指定的处理程序
	stopListener *StopListener
	handler      ServerHandler
}

func NewServer(handler ServerHandler) (server *RedisServer) {
	server = &RedisServer{}
	server.SetHandler(handler)
	return
}

func (server *RedisServer) SetHandler(handler ServerHandler) {
	server.handler = handler
}

// 开始监听主机端口
// @host "localhost:6379"
func (server *RedisServer) Listen(host string) error {
	l, err := net.Listen("tcp", host)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	server.stopListener = stopListener

	if server.handler == nil {
		return errors.New("handler undefined")
	}

	// run loop
	for {
		conn, err := server.stopListener.Accept()
		if err != nil {
			log.Printf("Accept failed: %s\n", err)
			return err
		}
		// go
		go server.handleConnection(NewSession(conn))
	}

	return nil
}

// 处理一个客户端连接
func (server *RedisServer) handleConnection(session *Session) {
	var err error

	defer func() {
		// 异常处理
		if v := recover(); v != nil {
			var ok bool
			if err, ok = v.(error); !ok {
				err = errors.New(fmt.Sprint(v))
			}
			session.Close()
			server.handler.ExceptionCaught(err)
		}
		// 连接终止
		server.handler.SessionClosed(session, err)
	}()

	server.handler.SessionOpened(session)

	for {
		var cmd *Command
		cmd, err = session.ReadCommand()
		// 常见的error是:
		// 1) io.EOF
		// 2) read tcp 127.0.0.1:51863: connection reset by peer
		if err != nil {
			session.Close()
			break
		}
		// 处理
		reply := server.handler.On(session, cmd)
		if reply != nil {
			err = session.WriteReply(reply)
			if err != nil {
				session.Close()
				break
			}
		}
	}
}

func (server *RedisServer) Stop() {
	log.Printf("redis entry stoping...")
	server.stopListener.Stop()
}
