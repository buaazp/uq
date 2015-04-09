package entry

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

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

type HttpQueueRequest struct {
	TopicName string `json:"topic"`
	LineName  string `json:"line,omitempty"`
	Recycle   string `json:"recycle,omitempty"`
}

func NewHttpEntry(host string, port int, messageQueue queue.MessageQueue) (*HttpEntry, error) {
	h := new(HttpEntry)

	router := mux.NewRouter()
	router.HandleFunc("/add", h.addHandler).Methods("POST")
	router.HandleFunc("/pop/{topic}/{line}", h.popHandler).Methods("GET")
	router.HandleFunc("/push/{topic}", h.pushHandler).Methods("POST")
	router.HandleFunc("/del", h.delHandler).Methods("POST")
	router.HandleFunc("/empty", h.emptyHandler).Methods("POST")

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
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	// Use test/genHttpJsonReq.go to generate json string
	// len = 47 json: {"topic":"foo","line":"x","recycle":"1h10m30s"}
	hqr := new(HttpQueueRequest)
	err = json.Unmarshal(data, hqr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	qr := new(queue.QueueRequest)
	qr.TopicName = hqr.TopicName
	qr.LineName = hqr.LineName
	if hqr.Recycle != "" {
		recycle, err := time.ParseDuration(hqr.Recycle)
		if err != nil {
			writeErrorHttp(w, NewError(
				ErrBadRequest,
				err.Error(),
			))
			return
		}
		qr.Recycle = recycle
	}

	err = h.messageQueue.Create(qr)
	if err != nil {
		writeErrorHttp(w, err)
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
		writeErrorHttp(w, err)
		return
	}
	if len(data) <= 0 {
		writeErrorHttp(w, NewError(
			ErrNone,
			err.Error(),
		))
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
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	err = h.messageQueue.Push(t, data)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) delHandler(w http.ResponseWriter, req *http.Request) {
	limitedr := NewLimitedBufferReader(req.Body, MaxBodyLength)
	data, err := ioutil.ReadAll(limitedr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}
	key := string(data)

	err = h.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHttp(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HttpEntry) emptyHandler(w http.ResponseWriter, req *http.Request) {
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
	// len = 55 json: {"TopicName":"foo","LineName":"x","Recycle":"1h10m30s"}
	hqr := new(HttpQueueRequest)
	err = json.Unmarshal(data, hqr)
	if err != nil {
		writeErrorHttp(w, NewError(
			ErrBadRequest,
			err.Error(),
		))
		return
	}

	qr := new(queue.QueueRequest)
	qr.TopicName = hqr.TopicName
	qr.LineName = hqr.LineName
	err = h.messageQueue.Empty(qr)
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
