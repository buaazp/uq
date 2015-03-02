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
	"github.com/buaazp/uq/etcd"
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
	flag.IntVar(&port, "p", 11211, "listen port")
	flag.StringVar(&front, "f", "mc", "frontend interface")
	flag.StringVar(&backend, "b", "leveldb", "store backend")
	flag.StringVar(&storePath, "d", "./data/uq.db", "store path")
	flag.StringVar(&etcdServer, "etcd", "", "etcd service location")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[uq] ")

	flag.Parse()
	SetIp(ip)
	SetPort(port)
	SetEtcdServer(etcdServer)
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

	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage)
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

	err = etcd.RegisterUq()
	if err != nil {
		entrance.Stop()
		wg.Wait()
		fmt.Printf("etcd set error: %s\n", err)
		return
	}

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
