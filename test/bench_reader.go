package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/buaazp/uq/utils"
)

var (
	runfor    = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	topic     = flag.String("topic", "foo", "topic to receive messages on")
	size      = flag.Int("size", 200, "size of messages")
	deadline  = flag.String("deadline", "", "deadline to start the benchmark run")
	storePath = flag.String("path", "./data", "store path")
)

var totalMsgCount int64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[uq_bench_reader] ")

	var err error
	var storage store.Storage
	// storage, err = store.NewMemStore()
	storage, err = store.NewLevelStore(*storePath)
	if err != nil {
		fmt.Printf("store init error: %s\n", err)
		return
	}

	var messageQueue queue.MessageQueue
	messageQueue, err = queue.NewUnitedQueue(storage, "", 0, nil, "uq")
	if err != nil {
		fmt.Printf("queue init error: %s\n", err)
		return
	}

	qr := new(queue.QueueRequest)
	qr.TopicName = *topic
	qr.LineName = "x"
	err = messageQueue.Create(qr)
	if err != nil {
		if e := err.(*Error); e.ErrorCode != ErrLineExisted {
			fmt.Printf("line create error: %s\n", err)
			return
		}
	}
	line := *topic + "/x"
	log.Printf("line: %s", line)

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < runtime.GOMAXPROCS(runtime.NumCPU()); j++ {
		log.Printf("runner: %d", j)
		wg.Add(1)
		go func() {
			subWorker(messageQueue, *runfor, line, rdyChan, goChan)
			wg.Done()
		}()
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))

	messageQueue.Close()
}

func subWorker(mq queue.MessageQueue, td time.Duration, line string, rdyChan chan int, goChan chan int) {
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	for {
		_, _, err := mq.Pop(line)
		if err != nil {
			// log.Printf("mq pop error: %s\n", err)
		}
		msgCount++
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
