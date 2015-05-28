package store

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ldb    Storage
	dbPath string
)

func init() {
	dbPath = os.TempDir() + "/uq.store.test.db"
}

func TestNewLevelStore(t *testing.T) {
	Convey("Test New Level Store", t, func() {
		ldb, err = NewLevelStore(dbPath)
		So(err, ShouldBeNil)
		So(ldb, ShouldNotBeNil)

		ldb2, err2 := NewLevelStore("/path_not_existed")
		So(err2, ShouldNotBeNil)
		So(ldb2, ShouldBeNil)
	})
}

func TestSetLevel(t *testing.T) {
	Convey("Test Level Store Set", t, func() {
		err = ldb.Set("foo", []byte("bar"))
		So(err, ShouldBeNil)
	})
}

func TestGetLevel(t *testing.T) {
	Convey("Test Level Store Get", t, func() {
		data, err := ldb.Get("foo")
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "bar")
	})
}

func TestDelLevel(t *testing.T) {
	Convey("Test Level Store Del", t, func() {
		err = ldb.Del("foo")
		So(err, ShouldBeNil)
	})
}

func TestCloseLevel(t *testing.T) {
	Convey("Test Level Store Close", t, func() {
		err = ldb.Close()
		So(err, ShouldBeNil)

		err = ldb.Close()
		So(err, ShouldNotBeNil)

		err = os.RemoveAll(dbPath)
		So(err, ShouldBeNil)
	})
}
