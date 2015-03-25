package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAcatui(t *testing.T) {
	Convey("Test Acatui", t, func() {
		key := "foo/x"
		var i uint64 = 100
		str := Acatui(key, "/", i)
		So(str, ShouldEqual, "foo/x/100")
	})
}

func TestAcati(t *testing.T) {
	Convey("Test Acati", t, func() {
		key := "foo/x"
		var i int = 100
		str := Acati(key, "/", i)
		So(str, ShouldEqual, "foo/x/100")
	})
}

func TestAddrcat(t *testing.T) {
	Convey("Test Addrcat", t, func() {
		addr := "localhost"
		var port int = 9689
		str := Addrcat(addr, port)
		So(str, ShouldEqual, "localhost:9689")
	})
}

func TestAtoi(t *testing.T) {
	Convey("Test Atoi", t, func() {
		str := "31415926"
		num := Atoi(str)
		So(num, ShouldEqual, 31415926)
		str2 := ""
		num2 := Atoi(str2)
		So(num2, ShouldEqual, 0)
		str3 := "abcd"
		num3 := Atoi(str3)
		So(num3, ShouldEqual, 0)
	})
}
