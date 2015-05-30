package entry

import (
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/utils"
)

const (
	queuePrefixV1 = "/v1/queues"
)

// HTTPEntry is the HTTP entrance of uq
type HTTPEntry struct {
	host         string
	port         int
	server       *http.Server
	stopListener *utils.StopListener
	messageQueue queue.MessageQueue
}

// NewHTTPEntry returns a new HTTPEntry server
func NewHTTPEntry(host string, port int, messageQueue queue.MessageQueue) (*HTTPEntry, error) {
	h := new(HTTPEntry)

	addr := utils.Addrcat(host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = h

	h.host = host
	h.port = port
	h.server = server
	h.messageQueue = messageQueue

	return h, nil
}

// ServeHTTP implements the ServeHTTP interface of HTTPEntry
func (h *HTTPEntry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !utils.AllowMethod(w, req.Method, "HEAD", "GET", "POST", "PUT", "DELETE") {
		return
	}

	if strings.HasPrefix(req.URL.Path, queuePrefixV1) {
		key := req.URL.Path[len(queuePrefixV1):]
		h.queueHandler(w, req, key)
		return
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
	return
}

func (h *HTTPEntry) queueHandler(w http.ResponseWriter, req *http.Request, key string) {
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

func writeErrorHTTP(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *utils.Error:
		e.WriteTo(w)
	default:
		// log.Printf("unexpected error: %v", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
	}
}

func (h *HTTPEntry) addHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHTTP(w, utils.NewError(
			utils.ErrInternalError,
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
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *HTTPEntry) pushHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHTTP(w, utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		))
		return
	}

	data := []byte(req.FormValue("value"))
	err = h.messageQueue.Push(key, data)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPEntry) popHandler(w http.ResponseWriter, req *http.Request, key string) {
	id, data, err := h.messageQueue.Pop(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-UQ-ID", id)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *HTTPEntry) delHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := h.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListenAndServe implements the ListenAndServe interface
func (h *HTTPEntry) ListenAndServe() error {
	addr := utils.Addrcat(h.host, h.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := utils.NewStopListener(l)
	if err != nil {
		return err
	}
	h.stopListener = stopListener

	log.Printf("http entrance serving at %s...", addr)
	return h.server.Serve(h.stopListener)
}

// Stop implements the Stop interface
func (h *HTTPEntry) Stop() {
	log.Printf("http entry stoping...")
	h.stopListener.Stop()
	h.messageQueue.Close()
}
