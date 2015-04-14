package utils

import (
	"bytes"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestLimitedBufferReaderRead(t *testing.T) {
	Convey("Test LimitedBufferReader", t, func() {
		buf := bytes.NewBuffer(make([]byte, 10))
		ln := 1
		lr := NewLimitedBufferReader(buf, ln)
		n, err := lr.Read(make([]byte, 10))
		So(err, ShouldBeNil)
		So(n, ShouldEqual, ln)
	})
}
