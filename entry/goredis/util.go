package goredis

import (
	"strconv"
)

// 缓存下标对应的字符串
var itoaNums []string

func init() {
	itoaNums = make([]string, 1000)
	for i, count := 0, len(itoaNums); i < count; i++ {
		itoaNums[i] = strconv.Itoa(i)
	}
}

// 经过缓存优化的itoa函数，减少strconv.Itoa的调用
func itoa(i int) string {
	if i > 0 && i < len(itoaNums) {
		return itoaNums[i]
	} else {
		return strconv.Itoa(i)
	}
}
