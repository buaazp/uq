package store

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	mdb Storage
	err error
)

func TestNewMemStore(t *testing.T) {
	Convey("Test New Mem Store", t, func() {
		mdb, err = NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)
	})
}

func TestSetMem(t *testing.T) {
	Convey("Test Mem Store Set", t, func() {
		err = mdb.Set("foo", []byte("bar"))
		So(err, ShouldBeNil)
	})
}

func TestGetMem(t *testing.T) {
	Convey("Test Mem Store Get", t, func() {
		data, err := mdb.Get("foo")
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "bar")

		data2, err := mdb.Get("bar")
		So(err, ShouldNotBeNil)
		So(data2, ShouldBeNil)
	})
}

func TestDelMem(t *testing.T) {
	Convey("Test Mem Store Del", t, func() {
		err = mdb.Del("foo")
		So(err, ShouldBeNil)

		err = mdb.Del("bar")
		So(err, ShouldNotBeNil)
	})
}

func TestCloseMem(t *testing.T) {
	Convey("Test Mem Store Close", t, func() {
		err = mdb.Close()
		So(err, ShouldBeNil)
	})
}
