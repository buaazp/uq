package main

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestArgs(t *testing.T) {
	Convey("Test UQ CMD Args", t, func() {
		So(checkArgs(), ShouldEqual, true)
		db = "mysql"
		So(checkArgs(), ShouldEqual, false)
		db = "memdb"
		protocol = "http2"
		So(checkArgs(), ShouldEqual, false)
	})
}
