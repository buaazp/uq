package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/buaazp/uq/entry"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
)

var (
	host      string
	port      int
	backend   string
	storePath string
)

func init() {
	flag.StringVar(&host, "h", "0.0.0.0", "listen ip")
	flag.IntVar(&port, "p", 11211, "listen port")
	flag.StringVar(&backend, "b", "memory", "store backend")
	flag.StringVar(&storePath, "s", "./data/uq.db", "store path")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.Printf("smq started!")

	flag.Parse()

	var err error

	var storage store.Storage
	// storage, err = store.NewMemStore()
	storage, err = store.NewLevelStore(storePath)
	if err != nil {
		log.Printf("store init error: %s", err)
		return
	}

	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage)
	if err != nil {
		log.Printf("queue init error: %s", err)
		return
	}

	var entrance entry.Entrance
	entrance, err = entry.NewHttpEntry(host, port, messageQueue)
	if err != nil {
		log.Printf("entry init error: %s", err)
		return
	}

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGINT)
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		defer wg.Done()
		entrance.ListenAndServe()
	}()

	log.Printf("Serving Entrance...")
	select {
	case signal := <-stop:
		log.Printf("got signal:%v", signal)
	}
	log.Printf("Stopping entrance...")
	entrance.Stop()
	log.Printf("Waiting on server...")
	wg.Wait()
}
