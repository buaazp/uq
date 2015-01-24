package entry

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/buaazp/uq/queue"
	"github.com/coreos/etcd/pkg/ioutils"
	"github.com/gorilla/mux"
)

type HttpEntry struct {
	Host         string
	Port         int
	MaxSize      int
	Server       *http.Server
	StopListener *StopListener
	MessageQueue queue.MessageQueue
}

func NewHttpEntry(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	router := mux.NewRouter()
	router.HandleFunc("/create", h.createHandler).Methods("POST")
	router.HandleFunc("/pop/{topic}/{line}", h.popHandler).Methods("GET")
	router.HandleFunc("/push/{topic}", h.pushHandler).Methods("POST")

	addr := fmt.Sprintf("%s:%d", host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = router

	h.Host = host
	h.Port = port
	h.MaxSize = 10 * 1024 * 1024
	h.Server = server
	h.MessageQueue = messageQueue

	return h, nil
}

func (h *HttpEntry) createHandler(w http.ResponseWriter, req *http.Request) {
	limitedr := ioutils.NewLimitedBufferReader(req.Body, h.MaxSize)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		http.Error(w, "400 Bad Request!", http.StatusBadRequest)
		return
	}

	name := string(data)
	err = h.MessageQueue.Create(name)
	if err != nil {
		log.Printf("push error: %s", err)
		http.Error(w, "500 Bad Request!", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) popHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["topic"]
	l := vars["line"]
	key := fmt.Sprintf("%s/%s", t, l)

	data, err := h.MessageQueue.Pop(key)
	if err != nil || len(data) <= 0 {
		// log.Printf("pop error: %s", err)
		http.Error(w, "404 Not Found!", http.StatusNotFound)
		return
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

func (h *HttpEntry) pushHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["topic"]

	limitedr := ioutils.NewLimitedBufferReader(req.Body, h.MaxSize)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		http.Error(w, "400 Bad Request!", http.StatusBadRequest)
		return
	}

	err = h.MessageQueue.Push(t, data)
	if err != nil {
		log.Printf("push error: %s", err)
		http.Error(w, "500 Bad Request!", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) ListenAndServe() error {
	ln, err := net.Listen("tcp", h.Server.Addr)
	if err != nil {
		return err
	}

	stopListener, err := NewStopListener(ln)
	if err != nil {
		return err
	}
	h.StopListener = stopListener

	return h.Server.Serve(h.StopListener)
}

func (h *HttpEntry) Stop() {
	h.MessageQueue.Close()
	h.StopListener.Stop()
}
