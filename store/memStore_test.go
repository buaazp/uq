package store

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewMemStore(t *testing.T) {
	Convey("Test New Mem Store", t, func() {
		mdb, err := NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)
	})
}

func TestSetMem(t *testing.T) {
	Convey("Test Mem Store Set", t, func() {
		mdb, err := NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		err = mdb.Set("foo", []byte("bar"))
		So(err, ShouldBeNil)
	})
}

func TestGetMem(t *testing.T) {
	Convey("Test Mem Store Get", t, func() {
		mdb, err := NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		err = mdb.Set("foo", []byte("bar"))
		So(err, ShouldBeNil)

		data, err := mdb.Get("foo")
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "bar")
	})
}

func TestDelMem(t *testing.T) {
	Convey("Test Mem Store Del", t, func() {
		mdb, err := NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		err = mdb.Set("foo", []byte("bar"))
		So(err, ShouldBeNil)

		err = mdb.Del("foo")
		So(err, ShouldBeNil)
	})
}
