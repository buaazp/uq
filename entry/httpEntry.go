package entry

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/buaazp/uq/queue"
	. "github.com/buaazp/uq/utils"
	"github.com/gorilla/mux"
)

type HttpEntry struct {
	host         string
	port         int
	server       *http.Server
	stopListener *StopListener
	messageQueue queue.MessageQueue
}

func NewHttpEntry(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	router := mux.NewRouter()
	router.HandleFunc("/add", h.addHandler).Methods("POST")
	router.HandleFunc("/pop/{topic}/{line}", h.popHandler).Methods("GET")
	router.HandleFunc("/push/{topic}", h.pushHandler).Methods("POST")
	router.HandleFunc("/del", h.delHandler).Methods("POST")

	addr := Addrcat(host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = router

	h.host = host
	h.port = port
	h.server = server
	h.messageQueue = messageQueue

	return h, nil
}

func (h *HttpEntry) addHandler(w http.ResponseWriter, req *http.Request) {
	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		http.Error(w, "400 Bad Request!\r\n"+err.Error(), http.StatusBadRequest)
		return
	}

	// len: 56
	// json: {"TopicName":"foo","LineName":"x","Recycle":10000000000}
	cr := new(queue.QueueRequest)
	err = json.Unmarshal(data, cr)
	if err != nil {
		log.Printf("create error: %s", err)
		http.Error(w, "400 Bad Request!\r\n"+err.Error(), http.StatusBadRequest)
		return
	}

	err = h.messageQueue.Create(cr)
	if err != nil {
		log.Printf("create error: %s", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) popHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["topic"]
	l := vars["line"]
	key := t + "/" + l

	id, data, err := h.messageQueue.Pop(key)
	if err != nil {
		// log.Printf("pop error: %s", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
		return
	}
	if len(data) <= 0 {
		http.Error(w, "404 Not Found!", http.StatusNotFound)
		return
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-UQ-MessageID", strconv.FormatUint(id, 10))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

func (h *HttpEntry) pushHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["topic"]

	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		http.Error(w, "400 Bad Request!\r\n"+err.Error(), http.StatusBadRequest)
		return
	}

	err = h.messageQueue.Push(t, data)
	if err != nil {
		log.Printf("push error: %s", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) delHandler(w http.ResponseWriter, req *http.Request) {
	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		http.Error(w, "400 Bad Request!\r\n"+err.Error(), http.StatusBadRequest)
		return
	}
	key := string(data)

	err = h.messageQueue.Confirm(key)
	if err != nil {
		log.Printf("confirm error: %s", err)
		http.Error(w, "500 Internal Error!\r\n"+err.Error(), http.StatusInternalServerError)
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
