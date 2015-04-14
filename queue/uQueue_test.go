package queue

import (
	"os"
	"strconv"
	"testing"

	"github.com/buaazp/uq/store"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dbPath = "/tmp/uq.queue.test.db"
)

var (
	err error
	ldb store.Storage
	uq  *UnitedQueue
)

func TestNewUnitedQueue(t *testing.T) {
	Convey("Test New Uq", t, func() {
		ldb, err = store.NewLevelStore(dbPath)
		So(ldb, ShouldNotBeNil)
		So(err, ShouldBeNil)

		uq, err = NewUnitedQueue(ldb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
	})
}

func TestCreateTopic(t *testing.T) {
	Convey("Test Create a Topic", t, func() {
		err = uq.Create("foo", "")
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		err = uq.Create("zp", "")
		So(err, ShouldBeNil)

		topiczp := uq.topics["zp"]
		So(topiczp, ShouldNotBeNil)
	})
}

func TestCreateLine(t *testing.T) {
	Convey("Test Create a Line", t, func() {
		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		err = uq.Create("foo/x", "")
		So(err, ShouldBeNil)

		linex := topic.lines["x"]
		So(linex, ShouldNotBeNil)

		err = uq.Create("foo/y", "3s")
		So(err, ShouldBeNil)

		liney := topic.lines["y"]
		So(liney, ShouldNotBeNil)

		topiczp := uq.topics["zp"]
		So(topiczp, ShouldNotBeNil)

		err = uq.Create("zp/z", "")
		So(err, ShouldBeNil)

		linez := topiczp.lines["z"]
		So(linez, ShouldNotBeNil)
	})
}

func TestPush(t *testing.T) {
	Convey("Test Push a Message", t, func() {
		data := []byte("1")
		err = uq.Push("foo", data)
		So(err, ShouldBeNil)
	})
}

func TestMultiPush(t *testing.T) {
	Convey("Test Multi Push Messages", t, func() {
		datas := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			datas[i] = []byte(strconv.Itoa(i + 2))
		}
		err = uq.MultiPush("foo", datas)
		So(err, ShouldBeNil)
	})
}

func TestPop(t *testing.T) {
	Convey("Test Pop a Message", t, func() {
		_, msg, err := uq.Pop("foo/x")
		So(err, ShouldBeNil)
		So(string(msg), ShouldEqual, "1")
	})
}

func TestMultiPop(t *testing.T) {
	Convey("Test Multi Pop Messages", t, func() {
		_, msgs, err := uq.MultiPop("foo/x", 5)
		So(err, ShouldBeNil)
		for i := 0; i < 5; i++ {
			So(string(msgs[i]), ShouldEqual, strconv.Itoa(i+2))
		}
	})
}

func TestConfirm(t *testing.T) {
	Convey("Test Confirm a Message", t, func() {
		id, msg, err := uq.Pop("foo/y")
		So(err, ShouldBeNil)
		So(string(msg), ShouldEqual, "1")

		err = uq.Confirm(id)
		So(err, ShouldBeNil)
	})
}

func TestMultiConfirm(t *testing.T) {
	Convey("Test Multi Confirm Messages", t, func() {
		ids, msgs, err := uq.MultiPop("foo/y", 5)
		So(err, ShouldBeNil)
		for i := 0; i < 5; i++ {
			So(string(msgs[i]), ShouldEqual, strconv.Itoa(i+2))
		}

		errs := uq.MultiConfirm(ids)
		for _, err := range errs {
			So(err, ShouldBeNil)
		}
	})
}

func TestStat(t *testing.T) {
	Convey("Test Stat Line", t, func() {
		key := "foo/y"
		qs, err := uq.Stat(key)
		So(err, ShouldBeNil)
		So(qs.Name, ShouldEqual, key)
	})
	Convey("Test Stat Topic", t, func() {
		key := "foo"
		qs, err := uq.Stat(key)
		So(err, ShouldBeNil)
		So(qs.Name, ShouldEqual, key)
	})
}

func TestEmpty(t *testing.T) {
	Convey("Test Empty Line", t, func() {
		key := "foo/y"
		err := uq.Empty(key)
		So(err, ShouldBeNil)
	})

	Convey("Test Empty Topic", t, func() {
		key := "foo"
		err := uq.Empty(key)
		So(err, ShouldBeNil)
	})
}

func TestRemove(t *testing.T) {
	Convey("Test Remove Line", t, func() {
		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		key := "foo/y"
		err := uq.Remove(key)
		So(err, ShouldBeNil)

		line2 := topic.lines["y"]
		So(line2, ShouldBeNil)
	})

	Convey("Test Empty Topic", t, func() {
		key := "foo"
		err := uq.Remove(key)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldBeNil)
	})
}

func TestClose(t *testing.T) {
	Convey("Test Close Queue", t, func() {
		uq.Close()
	})
}

func TestLoad(t *testing.T) {
	Convey("Test Load Queue", t, func() {
		ldb, err = store.NewLevelStore(dbPath)
		So(ldb, ShouldNotBeNil)
		So(err, ShouldBeNil)

		uq, err = NewUnitedQueue(ldb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)

		uq.Close()

		err = os.RemoveAll(dbPath)
		So(err, ShouldBeNil)
	})
}
