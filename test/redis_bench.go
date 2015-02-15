package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/garyburd/redigo/redis"
)

var host, method, topicName, lineName string
var port, testCount, concurrency, dataSize, bucket int

func init() {
	flag.StringVar(&host, "h", "127.0.0.1", "hostname")
	flag.IntVar(&port, "p", 11211, "port")
	flag.IntVar(&concurrency, "c", 10, "concurrency level")
	flag.IntVar(&testCount, "n", 10000, "test count")
	flag.IntVar(&dataSize, "d", 200, "data size")
	flag.StringVar(&method, "m", "push", "test method")
	flag.IntVar(&bucket, "b", 50, "bucket size when mpush")
	flag.StringVar(&topicName, "t", "StressTestTool", "topic to test")
	flag.StringVar(&lineName, "l", "Line", "line to test")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -h host -p port -c concurrency -n count -m push -t topic -l line\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func initQueue() error {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	if err != nil {
		log.Printf("redis conn error: %s", err)
		return err
	}
	defer conn.Close()

	_, err = conn.Do("QADD", topicName)
	if err != nil {
		log.Printf("add error: %v", err)
		return err
	}

	fullLineName := topicName + "/" + lineName
	_, err = conn.Do("QADD", fullLineName)
	if err != nil {
		log.Printf("add error: %v", err)
		return err
	}
	return nil
}

func setTestSingle(ch chan bool, cn, n int) error {
	var err error
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	if err != nil {
		log.Printf("redis conn error: %s", err)
		return err
	}
	defer conn.Close()
	v := make([]byte, dataSize)
	for i := 0; i < n; i++ {
		start := time.Now()
		_, err = conn.Do("QPUSH", topicName, v)
		if err != nil {
			log.Printf("set error: c%d %v", cn, err)
		} else {
			end := time.Now()
			duration := end.Sub(start).Seconds()
			log.Printf("set succ: %s spend: %.3fms", topicName, duration*1000)
		}
	}
	ch <- true
	return nil
}

func setTest(c, n int) {
	ch := make(chan bool)
	singleCount := n / c
	for i := 0; i < c; i++ {
		go setTestSingle(ch, i, singleCount)
	}
	for i := 0; i < c; i++ {
		select {
		case <-ch:
			log.Printf("set single succ: %s - c%d", topicName, i)
		}
	}
}

func msetTestSingle(ch chan bool, cn, n int) error {
	var err error
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	if err != nil {
		log.Printf("redis conn error: %s", err)
		return err
	}
	defer conn.Close()
	v := make([]byte, dataSize)
	b := make([]interface{}, bucket+1)
	b[0] = topicName
	for i := 1; i < bucket+1; i++ {
		b[i] = v
	}
	count := n / bucket
	for i := 0; i < count; i++ {
		start := time.Now()
		_, err = conn.Do("QMPUSH", b...)
		if err != nil {
			log.Printf("set error: c%d %v", cn, err)
		} else {
			end := time.Now()
			duration := end.Sub(start).Seconds()
			log.Printf("set succ: %s spend: %.3fms", topicName, duration*1000)
		}
	}
	ch <- true
	return nil
}

func msetTest(c, n int) {
	ch := make(chan bool)
	singleCount := n / c
	for i := 0; i < c; i++ {
		go msetTestSingle(ch, i, singleCount)
	}
	for i := 0; i < c; i++ {
		select {
		case <-ch:
			log.Printf("set single succ: %s - c%d", topicName, i)
		}
	}
}

func getTestSingle(ch chan bool, cn, n int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	if err != nil {
		log.Printf("redis conn error: %s", err)
		return err
	}
	defer conn.Close()
	key := topicName + "/" + lineName
	for i := 0; i < n; i++ {
		start := time.Now()
		reply, err := conn.Do("QPOP", key)
		if err != nil {
			log.Printf("get error: c%d %v", cn, err)
		} else {
			rpl, err := redis.Values(reply, err)
			if err != nil {
				fmt.Printf("redis.values error: c%d %v\n", cn, err)
				return err
			}
			// fmt.Printf("redis.strings %v\n", v)
			end := time.Now()
			duration := end.Sub(start).Seconds()
			id := uint64(rpl[1].(int64))
			log.Printf("get succ: %d spend: %.3fms", id, duration*1000)
		}
	}
	ch <- true
	return nil
}

func getTest(c, n int) {
	ch := make(chan bool)
	singleCount := n / c
	for i := 0; i < c; i++ {
		go getTestSingle(ch, i, singleCount)
	}
	for i := 0; i < c; i++ {
		select {
		case <-ch:
			log.Printf("get single succ: %s/%s - c%d", topicName, lineName, i)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if method != "mpush" && method != "push" && method != "pop" {
		fmt.Printf("test method not supported!\n")
		return
	}

	now := time.Now()
	year := now.Year()
	month := now.Month()
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	second := now.Second()
	logName := fmt.Sprintf("uq_redis_%s_%d-%d-%d_%d:%d:%d_c%d_n%d.log", method, year, month, day, hour, minute, second, concurrency, testCount)
	logfile, err := os.OpenFile(logName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}
	defer logfile.Close()

	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(logfile)

	err = initQueue()
	if err != nil {
		fmt.Printf("init queue error: %s\n", err)
		// return
	}

	start := time.Now()
	if method == "push" {
		setTest(concurrency, testCount)
	} else if method == "mpush" {
		msetTest(concurrency, testCount)
	} else if method == "pop" {
		getTest(concurrency, testCount)
	}
	end := time.Now()

	duration := end.Sub(start)
	dSecond := duration.Seconds()

	fmt.Printf("StressTest Done! ")
	fmt.Printf("Spend: %.3fs Speed: %.3f msg/s Throughput: %.3f MB/s", dSecond, float64(testCount)/dSecond, float64(testCount*dataSize)/(1024*1024*dSecond))

	log.Printf("StressTest Done!")
	log.Printf("Spend: %.3fs Speed: %.3f msg/s Throughput: %.3f MB/s", dSecond, float64(testCount)/dSecond, float64(testCount*dataSize)/(1024*1024*dSecond))
	return
}
