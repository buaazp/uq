package queue

import (
	"strconv"
	"testing"
	"time"

	"github.com/buaazp/uq/store"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewUnitedQueue(t *testing.T) {
	Convey("Test New Uq", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		uq.Close()
	})
}

func TestCreateTopic(t *testing.T) {
	Convey("Test Create a Topic", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)
	})
}

func TestCreateLine(t *testing.T) {
	Convey("Test Create a Line", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		qr = new(QueueRequest)
		qr.TopicName = "foo"
		qr.LineName = "x"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		line := topic.lines["x"]
		So(line, ShouldNotBeNil)
	})
}

func TestPush(t *testing.T) {
	Convey("Test Push a Message", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		err = uq.Push("foo", data)
		So(err, ShouldBeNil)
	})
}

func TestMultiPush(t *testing.T) {
	Convey("Test Multi Push Messages", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		datas := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			datas[i] = data
		}
		err = uq.MultiPush("foo", datas)
		So(err, ShouldBeNil)
	})
}

func TestPop(t *testing.T) {
	Convey("Test Pop a Message", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		err = uq.Push("foo", data)
		So(err, ShouldBeNil)

		qr = new(QueueRequest)
		qr.TopicName = "foo"
		qr.LineName = "x"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		line := topic.lines["x"]
		So(line, ShouldNotBeNil)

		_, msg, err := uq.Pop("foo/x")
		So(err, ShouldBeNil)
		So(string(msg), ShouldEqual, "bar")
	})
}

func TestMultiPop(t *testing.T) {
	Convey("Test Multi Pop Messages", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		datas := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			datas[i] = data
		}
		err = uq.MultiPush("foo", datas)
		So(err, ShouldBeNil)

		qr = new(QueueRequest)
		qr.TopicName = "foo"
		qr.LineName = "x"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		line := topic.lines["x"]
		So(line, ShouldNotBeNil)

		_, msgs, err := uq.MultiPop("foo/x", 5)
		So(err, ShouldBeNil)
		for i := 0; i < 5; i++ {
			So(string(msgs[i]), ShouldEqual, "bar")
		}
	})
}

func TestConfirm(t *testing.T) {
	Convey("Test Confirm a Message", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		err = uq.Push("foo", data)
		So(err, ShouldBeNil)

		qr = new(QueueRequest)
		qr.TopicName = "foo"
		qr.LineName = "x"
		qr.Recycle = 10 * time.Second
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		line := topic.lines["x"]
		So(line, ShouldNotBeNil)

		id, msg, err := uq.Pop("foo/x")
		So(err, ShouldBeNil)
		So(string(msg), ShouldEqual, "bar")

		key := "foo/x/" + strconv.FormatUint(id, 10)
		err = uq.Confirm(key)
		So(err, ShouldBeNil)
	})
}

func TestMultiConfirm(t *testing.T) {
	Convey("Test Multi Confirm Messages", t, func() {
		mdb, err := store.NewMemStore()
		So(err, ShouldBeNil)
		So(mdb, ShouldNotBeNil)

		uq, err := NewUnitedQueue(mdb, "127.0.0.1", 9689, nil, "uq")
		So(err, ShouldBeNil)
		So(uq, ShouldNotBeNil)
		defer uq.Close()

		qr := new(QueueRequest)
		qr.TopicName = "foo"
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		topic := uq.topics["foo"]
		So(topic, ShouldNotBeNil)

		data := []byte("bar")
		datas := make([][]byte, 5)
		for i := 0; i < 5; i++ {
			datas[i] = data
		}
		err = uq.MultiPush("foo", datas)
		So(err, ShouldBeNil)

		qr = new(QueueRequest)
		qr.TopicName = "foo"
		qr.LineName = "x"
		qr.Recycle = 10 * time.Second
		err = uq.Create(qr)
		So(err, ShouldBeNil)

		line := topic.lines["x"]
		So(line, ShouldNotBeNil)

		ids, msgs, err := uq.MultiPop("foo/x", 5)
		So(err, ShouldBeNil)
		for i := 0; i < 5; i++ {
			So(string(msgs[i]), ShouldEqual, "bar")
		}

		n, err := uq.MultiConfirm("foo/x", ids)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 5)
	})
}
