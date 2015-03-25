package utils

import (
	"strconv"
	"strings"
)

func Acatui(str, b string, n uint64) string {
	ns := strconv.FormatUint(n, 10)
	return str + b + ns
}

func Acati(str, b string, n int) string {
	return Acatui(str, b, uint64(n))
}

func Addrcat(host string, port int) string {
	return Acati(host, ":", port)
}

func Atoi(str string) uint64 {
	str = strings.Trim(str, " ")
	if len(str) > 0 {
		i, err := strconv.ParseUint(str, 10, 0)
		if err != nil {
			return 0
		} else {
			return i
		}
	} else {
		return 0
	}
}
