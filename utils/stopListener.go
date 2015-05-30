package utils

import (
	"errors"
	"net"
	"time"
)

// StopListener is a stopable listener
type StopListener struct {
	*net.TCPListener          //Wrapped listener
	stop             chan int //Channel used only to indicate listener should shutdown
}

var errStopped = errors.New("Listener stopped")

// NewStopListener returns a new StopListener with a net listener
func NewStopListener(l net.Listener) (*StopListener, error) {
	tcpL, ok := l.(*net.TCPListener)

	if !ok {
		return nil, errors.New("Cannot wrap listener")
	}

	retval := &StopListener{}
	retval.TCPListener = tcpL
	retval.stop = make(chan int)

	return retval, nil
}

// Accept implements the Accept interface
func (sl *StopListener) Accept() (net.Conn, error) {
	for {
		//Wait up to one second for a new connection
		sl.SetDeadline(time.Now().Add(time.Second))

		newConn, err := sl.TCPListener.Accept()

		//Check for the channel being closed
		select {
		case <-sl.stop:
			return nil, errStopped
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		return newConn, err
	}
}

// Stop implements the Stop interface
func (sl *StopListener) Stop() {
	close(sl.stop)
}
