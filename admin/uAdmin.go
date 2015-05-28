package admin

import (
	"log"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"strings"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
)

const (
	queuePrefixV1      = "/v1/queues"
	adminPrefixV1      = "/v1/admin"
	pprofPrefixCmd     = "/debug/pprof/cmdline"
	pprofPrefixProfile = "/debug/pprof/profile"
	pprofPrefixSymbol  = "/debug/pprof/symbol"
	pprofPrefixIndex   = "/debug/pprof"
)

type HttpEntry struct {
	host         string
	port         int
	adminMux     map[string]func(http.ResponseWriter, *http.Request, string)
	server       *http.Server
	stopListener *StopListener
	messageQueue queue.MessageQueue
}

func NewAdminServer(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	h.adminMux = map[string]func(http.ResponseWriter, *http.Request, string){
		"/stat":  h.statHandler,
		"/empty": h.emptyHandler,
		"/rm":    h.rmHandler,
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
	if !AllowMethod(w, req.Method, "HEAD", "GET", "POST", "PUT", "DELETE") {
		return
	}

	if strings.HasPrefix(req.URL.Path, queuePrefixV1) {
		key := req.URL.Path[len(queuePrefixV1):]
		h.queueHandler(w, req, key)
		return
	} else if strings.HasPrefix(req.URL.Path, adminPrefixV1) {
		key := req.URL.Path[len(adminPrefixV1):]
		h.adminHandler(w, req, key)
		return
	} else if strings.HasPrefix(req.URL.Path, pprofPrefixCmd) {
		httpprof.Cmdline(w, req)
		return
	} else if strings.HasPrefix(req.URL.Path, pprofPrefixProfile) {
		httpprof.Profile(w, req)
		return
	} else if strings.HasPrefix(req.URL.Path, pprofPrefixSymbol) {
		httpprof.Symbol(w, req)
		return
	} else if strings.HasPrefix(req.URL.Path, pprofPrefixIndex) {
		httpprof.Index(w, req)
		return
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

func (h *HttpEntry) queueHandler(w http.ResponseWriter, req *http.Request, key string) {
	switch req.Method {
	case "PUT":
		h.addHandler(w, req, key)
	case "POST":
		h.pushHandler(w, req, key)
	case "GET":
		h.popHandler(w, req, key)
	case "DELETE":
		h.delHandler(w, req, key)
	default:
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
	}
	return
}

func (h *HttpEntry) adminHandler(w http.ResponseWriter, req *http.Request, key string) {
	for prefix, handler := range h.adminMux {
		if strings.HasPrefix(key, prefix) {
			key = key[len(prefix):]
			handler(w, req, key)
			return
		}
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

func writeErrorHttp(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *Error:
		e.WriteTo(w)
	default:
		// log.Printf("unexpected error: %v", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
	}
}

func (h *HttpEntry) addHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	topicName := req.FormValue("topic")
	lineName := req.FormValue("line")
	key = topicName + "/" + lineName
	recycle := req.FormValue("recycle")

	// log.Printf("creating... %s %s", key, recycle)
	err = h.messageQueue.Create(key, recycle)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *HttpEntry) pushHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	data := []byte(req.FormValue("value"))
	err = h.messageQueue.Push(key, data)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) popHandler(w http.ResponseWriter, req *http.Request, key string) {
	id, data, err := h.messageQueue.Pop(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-UQ-ID", id)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *HttpEntry) delHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := h.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) statHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "GET" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	qs, err := h.messageQueue.Stat(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}

	// log.Printf("qs: %v", qs)
	data, err := qs.ToJson()
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrInternalError,
			err.Error(),
		))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *HttpEntry) emptyHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := h.messageQueue.Empty(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) rmHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := h.messageQueue.Remove(key)
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

	log.Printf("admin server serving at %s...", addr)
	return h.server.Serve(h.stopListener)
}

func (h *HttpEntry) Stop() {
	log.Printf("admin server stoping...")
	h.stopListener.Stop()
}
