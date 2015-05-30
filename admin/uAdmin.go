package admin

import (
	"log"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"strings"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/utils"
)

const (
	queuePrefixV1      = "/v1/queues"
	adminPrefixV1      = "/v1/admin"
	pprofPrefixCmd     = "/debug/pprof/cmdline"
	pprofPrefixProfile = "/debug/pprof/profile"
	pprofPrefixSymbol  = "/debug/pprof/symbol"
	pprofPrefixIndex   = "/debug/pprof"
)

// UnitedAdmin is the HTTP admin server of uq
type UnitedAdmin struct {
	host         string
	port         int
	adminMux     map[string]func(http.ResponseWriter, *http.Request, string)
	server       *http.Server
	stopListener *utils.StopListener
	messageQueue queue.MessageQueue
}

// NewUnitedAdmin returns a UnitedAdmin
func NewUnitedAdmin(host string, port int, messageQueue queue.MessageQueue) (*UnitedAdmin, error) {
	s := new(UnitedAdmin)

	s.adminMux = map[string]func(http.ResponseWriter, *http.Request, string){
		"/stat":  s.statHandler,
		"/empty": s.emptyHandler,
		"/rm":    s.rmHandler,
	}

	addr := utils.Addrcat(host, port)
	server := new(http.Server)
	server.Addr = addr
	server.Handler = s

	s.host = host
	s.port = port
	s.server = server
	s.messageQueue = messageQueue

	return s, nil
}

func (s *UnitedAdmin) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !utils.AllowMethod(w, req.Method, "HEAD", "GET", "POST", "PUT", "DELETE") {
		return
	}

	if strings.HasPrefix(req.URL.Path, queuePrefixV1) {
		key := req.URL.Path[len(queuePrefixV1):]
		s.queueHandler(w, req, key)
		return
	} else if strings.HasPrefix(req.URL.Path, adminPrefixV1) {
		key := req.URL.Path[len(adminPrefixV1):]
		s.adminHandler(w, req, key)
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

func (s *UnitedAdmin) queueHandler(w http.ResponseWriter, req *http.Request, key string) {
	switch req.Method {
	case "PUT":
		s.addHandler(w, req, key)
	case "POST":
		s.pushHandler(w, req, key)
	case "GET":
		s.popHandler(w, req, key)
	case "DELETE":
		s.delHandler(w, req, key)
	default:
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
	}
	return
}

func (s *UnitedAdmin) adminHandler(w http.ResponseWriter, req *http.Request, key string) {
	for prefix, handler := range s.adminMux {
		if strings.HasPrefix(key, prefix) {
			key = key[len(prefix):]
			handler(w, req, key)
			return
		}
	}

	http.Error(w, "404 Not Found!", http.StatusNotFound)
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

func (s *UnitedAdmin) addHandler(w http.ResponseWriter, req *http.Request, key string) {
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
	err = s.messageQueue.Create(key, recycle)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *UnitedAdmin) pushHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := req.ParseForm()
	if err != nil {
		writeErrorHTTP(w, utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		))
		return
	}

	data := []byte(req.FormValue("value"))
	err = s.messageQueue.Push(key, data)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *UnitedAdmin) popHandler(w http.ResponseWriter, req *http.Request, key string) {
	id, data, err := s.messageQueue.Pop(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-UQ-ID", id)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (s *UnitedAdmin) delHandler(w http.ResponseWriter, req *http.Request, key string) {
	err := s.messageQueue.Confirm(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *UnitedAdmin) statHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "GET" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	qs, err := s.messageQueue.Stat(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}

	// log.Printf("qs: %v", qs)
	data, err := qs.ToJSON()
	if err != nil {
		writeErrorHTTP(w, utils.NewError(
			utils.ErrInternalError,
			err.Error(),
		))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (s *UnitedAdmin) emptyHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := s.messageQueue.Empty(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *UnitedAdmin) rmHandler(w http.ResponseWriter, req *http.Request, key string) {
	if req.Method != "DELETE" {
		http.Error(w, "405 Method Not Allowed!", http.StatusMethodNotAllowed)
		return
	}

	err := s.messageQueue.Remove(key)
	if err != nil {
		writeErrorHTTP(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListenAndServe implements the ListenAndServe interface
func (s *UnitedAdmin) ListenAndServe() error {
	addr := utils.Addrcat(s.host, s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	stopListener, err := utils.NewStopListener(l)
	if err != nil {
		return err
	}
	s.stopListener = stopListener

	log.Printf("admin server serving at %s...", addr)
	return s.server.Serve(s.stopListener)
}

// Stop implements the Stop interface
func (s *UnitedAdmin) Stop() {
	log.Printf("admin server stoping...")
	s.stopListener.Stop()
}
