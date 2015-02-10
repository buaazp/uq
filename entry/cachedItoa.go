package entry

import (
	"strconv"
)

var itoaNums []string

func init() {
	itoaNums = make([]string, 1000)
	for i, count := 0, len(itoaNums); i < count; i++ {
		itoaNums[i] = strconv.Itoa(i)
	}
}

func itoa(i int) string {
	if i > 0 && i < len(itoaNums) {
		return itoaNums[i]
	} else {
		return strconv.Itoa(i)
	}
}
