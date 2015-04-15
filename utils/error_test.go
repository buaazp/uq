package utils

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestError(t *testing.T) {
	Convey("Test Error", t, func() {
		err := NewError(
			ErrInternalError,
			`This is a test error`,
		)
		So(err.statusCode(), ShouldEqual, 500)
		So(err.Error(), ShouldEqual, "500 Internal Error (This is a test error)")
		So(err.toJsonString(), ShouldEqual,
			`{"errorCode":500,"message":"Internal Error","cause":"This is a test error"}`,
		)
	})
}
