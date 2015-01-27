package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
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
	fmt.Printf("smq started!\n")

	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)

	flag.Parse()

	var err error
	var storage store.Storage
	// storage, err = store.NewMemStore()
	storage, err = store.NewLevelStore(storePath)
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage)
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		return
	}

	var entrance entry.Entrance
	entrance, err = entry.NewHttpEntry(host, port, messageQueue)
	if err != nil {
		fmt.Printf("entry init error: %s\n", err)
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

	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()

	log.Printf("entrance serving...")
	select {
	case signal := <-stop:
		log.Printf("got signal:%v", signal)
	}
	log.Printf("entrance stoping...")
	entrance.Stop()
	wg.Wait()
	fmt.Printf("byebye! uq see u later! 😄\n")
}
