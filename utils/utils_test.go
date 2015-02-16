package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAcati(t *testing.T) {
	Convey("Test Acati", t, func() {
		key := "foo/x"
		str := Acati(key, "/", 100)
		So(str, ShouldEqual, "foo/x/100")
	})
}

func TestAtoi(t *testing.T) {
	Convey("Test Atoi", t, func() {
		str := "31415926"
		num := Atoi(str)
		So(num, ShouldEqual, 31415926)
	})
}
