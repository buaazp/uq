package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/buaazp/uq/admin"
	"github.com/buaazp/uq/entry"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
)

var (
	ip        string
	host      string
	port      int
	adminPort int
	protocol  string
	db        string
	dir       string
	logFile   string
	etcd      string
	cluster   string
)

func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "self ip/host address")
	flag.StringVar(&host, "host", "0.0.0.0", "listen ip")
	flag.IntVar(&port, "port", 8808, "listen port")
	flag.IntVar(&adminPort, "admin-port", 8809, "admin listen port")
	flag.StringVar(&protocol, "protocol", "redis", "frontend interface type [redis/mc/http]")
	flag.StringVar(&db, "db", "goleveldb", "backend storage type [goleveldb/memdb]")
	flag.StringVar(&dir, "dir", "./data", "backend storage path")
	flag.StringVar(&logFile, "log", "", "uq log path")
	flag.StringVar(&etcd, "etcd", "", "etcd service location")
	flag.StringVar(&cluster, "cluster", "uq", "cluster name in etcd")
}

func belong(single string, team []string) bool {
	for _, one := range team {
		if single == one {
			return true
		}
	}
	return false
}

func checkArgs() bool {
	if !belong(db, []string{"goleveldb", "memdb"}) {
		fmt.Printf("db mode %s is not supported!\n", db)
		return false
	}
	if !belong(protocol, []string{"redis", "mc", "http"}) {
		fmt.Printf("protocol %s is not supported!\n", protocol)
		return false
	}
	return true
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	defer func() {
		fmt.Printf("byebye! uq see u later! 😄\n")
	}()

	flag.Parse()

	if !checkArgs() {
		return
	}

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		fmt.Printf("mkdir %s error: %s\n", dir, err)
		return
	}
	if logFile != "" {
		logFile = path.Join(dir, "uq.log")
		logf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("log open error: %s\n", err)
			return
		}
		defer logf.Close()
		log.SetOutput(logf)
	}
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[uq] ")

	fmt.Printf("uq started! 😄\n")

	var storage store.Storage
	// if db == "rocksdb" {
	// 	dbpath := path.Clean(path.Join(dir, "uq.db"))
	// 	log.Printf("dbpath: %s", dbpath)
	// 	storage, err = store.NewRockStore(dbpath)
	// } else if db == "goleveldb" {
	if db == "goleveldb" {
		dbpath := path.Clean(path.Join(dir, "uq.db"))
		log.Printf("dbpath: %s", dbpath)
		storage, err = store.NewLevelStore(dbpath)
	} else if db == "memdb" {
		storage, err = store.NewMemStore()
	} else {
		fmt.Printf("store %s is not supported!\n", db)
		return
	}
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	var etcdServers []string
	if etcd != "" {
		etcdServers = strings.Split(etcd, ",")
		for i, etcdServer := range etcdServers {
			if !strings.HasPrefix(etcdServer, "http://") {
				etcdServers[i] = "http://" + etcdServer
			}
		}
	}
	var messageQueue queue.MessageQueue
	// messageQueue, err = queue.NewFakeQueue(storage, ip, port, etcdServers, cluster)
	// if err != nil {
	// 	fmt.Printf("queue init error: %s\n", err)
	// 	storage.Close()
	// 	return
	// }
	messageQueue, err = queue.NewUnitedQueue(storage, ip, port, etcdServers, cluster)
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		storage.Close()
		return
	}

	var entrance entry.Entrance
	if protocol == "http" {
		entrance, err = entry.NewHTTPEntry(host, port, messageQueue)
	} else if protocol == "mc" {
		entrance, err = entry.NewMcEntry(host, port, messageQueue)
	} else if protocol == "redis" {
		entrance, err = entry.NewRedisEntry(host, port, messageQueue)
	} else {
		fmt.Printf("protocol %s is not supported!\n", protocol)
		return
	}
	if err != nil {
		fmt.Printf("entry init error: %s\n", err)
		messageQueue.Close()
		return
	}

	stop := make(chan os.Signal)
	entryFailed := make(chan bool)
	adminFailed := make(chan bool)
	signal.Notify(stop, syscall.SIGINT, os.Interrupt, os.Kill)
	var wg sync.WaitGroup

	// start entrance server
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
	}(entryFailed)

	var adminServer admin.Administrator
	adminServer, err = admin.NewUnitedAdmin(host, adminPort, messageQueue)
	if err != nil {
		fmt.Printf("admin init error: %s\n", err)
		entrance.Stop()
		return
	}

	// start admin server
	go func(c chan bool) {
		wg.Add(1)
		defer wg.Done()
		err := adminServer.ListenAndServe()
		if err != nil {
			if !strings.Contains(err.Error(), "stopped") {
				fmt.Printf("entry listen error: %s\n", err)
			}
			close(c)
		}
	}(adminFailed)

	select {
	case <-stop:
		// log.Printf("got signal: %v", signal)
		adminServer.Stop()
		log.Printf("admin server stoped.")
		entrance.Stop()
		log.Printf("entrance stoped.")
	case <-entryFailed:
		messageQueue.Close()
	case <-adminFailed:
		entrance.Stop()
	}
	wg.Wait()
}
