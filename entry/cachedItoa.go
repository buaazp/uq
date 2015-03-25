package entry

import (
	"strconv"
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

func itoa(i int) string {
	if i > 0 && i < itoaCount {
		return itoaNums[i]
	} else {
		return strconv.Itoa(i)
	}
}
