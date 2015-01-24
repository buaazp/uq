package utils

import (
	"strconv"
	"strings"
)

func Atoi(str string) int {
	str = strings.Trim(str, " ")
	if len(str) > 0 {
		i, err := strconv.Atoi(str)
		if err != nil {
			return 0
		} else {
			return i
		}
	} else {
		return 0
	}
}
