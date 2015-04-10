package entry

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

type HttpEntry struct {
	host         string
	port         int
	mux          map[string]func(http.ResponseWriter, *http.Request, string)
	server       *http.Server
	stopListener *StopListener
	messageQueue queue.MessageQueue
}

func NewHttpEntry(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	h.mux = map[string]func(http.ResponseWriter, *http.Request, string){
		"/add":   h.addHandler,
		"/push":  h.pushHandler,
		"/pop":   h.popHandler,
		"/del":   h.delHandler,
		"/empty": h.emptyHandler,
	}

	addr := Addrcat(host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = h

	h.host = host
	h.port = port
	h.server = server
	h.messageQueue = messageQueue

	return h, nil
}

func (h *HttpEntry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	for prefix, handler := range h.mux {
		if strings.HasPrefix(req.URL.Path, prefix) {
			key := req.URL.Path[len(prefix):]
			key = strings.TrimPrefix(key, "/")
			key = strings.TrimSuffix(key, "/")
			handler(w, req, key)
			return
		}
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

// allowMethod verifies that the given method is one of the allowed methods,
// and if not, it writes an error to w.  A boolean is returned indicating
// whether or not the method is allowed.
func allowMethod(w http.ResponseWriter, m string, ms ...string) bool {
	for _, meth := range ms {
		if m == meth {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(ms, ","))
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	return false
}

func (h *HttpEntry) addHandler(w http.ResponseWriter, req *http.Request, key string) {
	if !allowMethod(w, req.Method, "PUT", "POST") {
		return
	}

	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	// Use test/genHttpJsonReq.go to generate json string
	// len = 47 json: {"topic":"foo","line":"x","recycle":"1h10m30s"}
	qr := new(queue.QueueRequest)
	err = json.Unmarshal(data, qr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	err = h.messageQueue.Create(qr)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) popHandler(w http.ResponseWriter, req *http.Request, key string) {
	if !allowMethod(w, req.Method, "HEAD", "GET") {
		return
	}

	id, data, err := h.messageQueue.Pop(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-UQ-ID", Acatui(key, "/", id))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *HttpEntry) pushHandler(w http.ResponseWriter, req *http.Request, key string) {
	if !allowMethod(w, req.Method, "PUT", "POST") {
		return
	}

	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	err = h.messageQueue.Push(key, data)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) delHandler(w http.ResponseWriter, req *http.Request, key string) {
	if !allowMethod(w, req.Method, "DELETE") {
		return
	}

	err := h.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) emptyHandler(w http.ResponseWriter, req *http.Request, key string) {
	if !allowMethod(w, req.Method, "DELETE") {
		return
	}

	err := h.messageQueue.Empty(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) ListenAndServe() error {
	addr := Addrcat(h.host, h.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(l)
	if err != nil {
		return err
	}
	h.stopListener = stopListener

	log.Printf("http entrance serving at %s...", addr)
	return h.server.Serve(h.stopListener)
}

func (h *HttpEntry) Stop() {
	log.Printf("http entry stoping...")
	h.stopListener.Stop()
	h.messageQueue.Close()
}

func writeErrorHttp(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *Error:
		e.WriteTo(w)
	default:
		log.Printf("unexpected error: %v", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
	}
}
