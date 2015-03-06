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
	"strings"
	"sync"

	"github.com/buaazp/uq/entry"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/buaazp/uq/utils"
)

var (
	ip         string
	host       string
	port       int
	front      string
	backend    string
	storePath  string
	etcdServer string
)

func init() {
	flag.StringVar(&ip, "a", "127.0.0.1", "self ip address")
	flag.StringVar(&host, "h", "0.0.0.0", "listen ip")
	flag.IntVar(&port, "p", 6379, "listen port")
	flag.StringVar(&front, "f", "redis", "frontend interface")
	flag.StringVar(&backend, "b", "leveldb", "store backend")
	flag.StringVar(&storePath, "d", "./data/uq.db", "store path")
	flag.StringVar(&etcdServer, "etcd", "", "etcd service location")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[uq] ")

	flag.Parse()
	fmt.Printf("uq started! 😄\n")

	var err error
	var storage store.Storage
	if backend == "leveldb" {
		storage, err = store.NewLevelStore(storePath)
	} else if backend == "memory" {
		storage, err = store.NewMemStore()
	}
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	var etcdServers []string
	if etcdServer != "" {
		etcdServers = strings.Split(etcdServer, ",")
	}
	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage, ip, port, etcdServers)
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		return
	}

	var entrance entry.Entrance
	if front == "http" {
		entrance, err = entry.NewHttpEntry(host, port, messageQueue)
	} else if front == "mc" {
		entrance, err = entry.NewMcEntry(host, port, messageQueue)
	} else if front == "redis" {
		entrance, err = entry.NewRedisEntry(host, port, messageQueue)
	}
	if err != nil {
		fmt.Printf("entry init error: %s\n", err)
		return
	}

	stop := make(chan os.Signal)
	failed := make(chan bool)
	// signal.Notify(stop, syscall.SIGINT, os.Interrupt, os.Kill)
	signal.Notify(stop)
	var wg sync.WaitGroup
	go func(c chan bool) {
		wg.Add(1)
		defer wg.Done()
		err := entrance.ListenAndServe()
		if err != nil {
			if !strings.Contains(err.Error(), "stopped") {
				fmt.Printf("entry listen error: %s\n", err)
			}
			close(c)
		}
	}(failed)

	go func() {
		addr := Addrcat(host, 8080)
		log.Println(http.ListenAndServe(addr, nil))
	}()

	select {
	case signal := <-stop:
		log.Printf("got signal: %v", signal)
		log.Printf("entrance stoping...")
		entrance.Stop()
	case <-failed:
		log.Printf("messageQueue stoping...")
		messageQueue.Close()
	}
	wg.Wait()
	fmt.Printf("byebye! uq see u later! 😄\n")
}
