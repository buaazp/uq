package utils

import (
	"strconv"
	"strings"
)

const (
	itoaCount int = 1000
)

var itoaNums []string

func init() {
	itoaNums = make([]string, itoaCount)
	for i, count := 0, len(itoaNums); i < count; i++ {
		itoaNums[i] = strconv.Itoa(i)
	}
}

// ItoaQuick returns the cached itoa string
func ItoaQuick(i int) string {
	if i > 0 && i < itoaCount {
		return itoaNums[i]
	}
	return strconv.Itoa(i)
}

// Acatui implements string cat uint64
func Acatui(str, b string, n uint64) string {
	ns := strconv.FormatUint(n, 10)
	return str + b + ns
}

// Acati implements string cat int
func Acati(str, b string, n int) string {
	return Acatui(str, b, uint64(n))
}

// Addrcat implements net address cat
func Addrcat(host string, port int) string {
	return Acati(host, ":", port)
}

// Atoi implements cached atoi
func Atoi(str string) uint64 {
	str = strings.Trim(str, " ")
	if len(str) > 0 {
		i, err := strconv.ParseUint(str, 10, 0)
		if err != nil {
			return 0
		}
		return i
	}
	return 0
}
