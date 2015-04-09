package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/buaazp/uq/entry"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/buaazp/uq/utils"
)

var (
	ip       string
	host     string
	port     int
	protocol string
	db       string
	dir      string
	etcd     string
	cluster  string
)

func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "self ip/host address")
	flag.StringVar(&host, "host", "0.0.0.0", "listen ip")
	flag.IntVar(&port, "port", 8808, "listen port")
	flag.StringVar(&protocol, "protocol", "redis", "frontend interface type [redis/mc/http]")
	flag.StringVar(&db, "db", "leveldb", "backend storage type [leveldb/memdb]")
	flag.StringVar(&dir, "dir", "./data", "backend storage path")
	flag.StringVar(&etcd, "etcd", "", "etcd service location")
	flag.StringVar(&cluster, "cluster", "uq", "cluster name in etcd")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[uq] ")

	flag.Parse()
	fmt.Printf("uq started! 😄\n")

	var err error
	var storage store.Storage
	if db == "leveldb" {
		dbpath := path.Clean(path.Join(dir, "uq.db"))
		log.Printf("dbpath: %s", dbpath)
		storage, err = store.NewLevelStore(dbpath)
	} else if db == "memdb" {
		storage, err = store.NewMemStore()
	}
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	var etcdServers []string
	if etcd != "" {
		etcdServers = strings.Split(etcd, ",")
	}
	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage, ip, port, etcdServers, cluster)
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		return
	}

	var entrance entry.Entrance
	if protocol == "http" {
		entrance, err = entry.NewHttpEntry(host, port, messageQueue)
	} else if protocol == "mc" {
		entrance, err = entry.NewMcEntry(host, port, messageQueue)
	} else if protocol == "redis" {
		entrance, err = entry.NewRedisEntry(host, port, messageQueue)
	}
	if err != nil {
		fmt.Printf("entry init error: %s\n", err)
		return
	}

	stop := make(chan os.Signal)
	failed := make(chan bool)
	signal.Notify(stop, syscall.SIGINT, os.Interrupt, os.Kill)
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
