package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// func Acatui(str, b string, n uint64) string {
// 	ns := strconv.FormatUint(n, 10)
// 	log.Printf("ns: %v len: %d", ns, len([]byte(ns)))
// 	log.Printf("ns: %v len: %d", ns, len(ns))

// 	bs := make([]byte, 8)
// 	// binary.LittleEndian.PutUint64(bs, n)
// 	binary.BigEndian.PutUint64(bs, n)
// 	log.Printf("bs: %v", string(bs))

// 	key := make([]byte, 0)
// 	key = append(key, []byte(str)...)
// 	key = append(key, []byte(b)...)
// 	key = append(key, bs...)

// 	rst := str + b + ns
// 	log.Printf("rst: %v", rst)
// 	log.Printf("key: %v", string(key))
// 	return rst
// }

// Acatui implements Adress cat uint64
func Acatui(str, b string, n uint64) string {
	ns := strconv.FormatUint(n, 10)
	return str + b + ns
}

// Fmt returns the fmt version of cat
func Fmt(str, b string, n uint64) string {
	return fmt.Sprintf("%s%s%d", str, b, n)
}

func main() {
	n := 1000000
	start := time.Now()
	for i := 0; i < n; i++ {
		_ = Acatui("foo", "/", 999)
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("Acatui duration: %v", duration)

	start2 := time.Now()
	for i := 0; i < n; i++ {
		_ = Fmt("foo", "/", 999)
	}
	end2 := time.Now()
	duration2 := end2.Sub(start2)
	log.Printf("Fmt duration: %v", duration2)
}
