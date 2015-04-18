package entry

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	storage      store.Storage
	messageQueue queue.MessageQueue
	entrance     Entrance
	client       *http.Client
)

func init() {
	client = new(http.Client)
}

func TestNewHttpEntry(t *testing.T) {
	Convey("Test New HTTP Entry", t, func() {
		var err error
		storage, err = store.NewMemStore()
		So(err, ShouldBeNil)
		So(storage, ShouldNotBeNil)
		messageQueue, err = queue.NewUnitedQueue(storage, "127.0.0.1", 8801, nil, "uq")
		So(err, ShouldBeNil)
		So(messageQueue, ShouldNotBeNil)

		entrance, err = NewHttpEntry("0.0.0.0", 8801, messageQueue)
		So(err, ShouldBeNil)
		So(entrance, ShouldNotBeNil)

		go func() {
			entrance.ListenAndServe()
		}()
	})
}

func TestHttpAdd(t *testing.T) {
	Convey("Test Http Add Api", t, func() {
		bf := bytes.NewBufferString("topic=foo")
		body := ioutil.NopCloser(bf)
		req, err := http.NewRequest(
			"PUT",
			"http://127.0.0.1:8801/v1/queues",
			body,
		)
		So(err, ShouldBeNil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusCreated)

		bf = bytes.NewBufferString("topic=foo&line=x&recycle=10s")
		body = ioutil.NopCloser(bf)
		req, err = http.NewRequest(
			"PUT",
			"http://127.0.0.1:8801/v1/queues",
			body,
		)
		So(err, ShouldBeNil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err = client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusCreated)
	})
}

func TestHttpPush(t *testing.T) {
	Convey("Test Http Push Api", t, func() {
		bf := bytes.NewBufferString("value=1")
		body := ioutil.NopCloser(bf)
		req, err := http.NewRequest(
			"POST",
			"http://127.0.0.1:8801/v1/queues/foo",
			body,
		)
		So(err, ShouldBeNil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestHttpPop(t *testing.T) {
	Convey("Test Http Pop Api", t, func() {
		req, err := http.NewRequest(
			"GET",
			"http://127.0.0.1:8801/v1/queues/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusOK)

		body, err := ioutil.ReadAll(resp.Body)
		So(err, ShouldBeNil)
		id := resp.Header.Get("X-UQ-ID")
		So(id, ShouldEqual, "foo/x/0")
		msg := string(body)
		So(msg, ShouldEqual, "1")
	})
}

func TestHttpConfirm(t *testing.T) {
	Convey("Test Http Confirm Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8801/v1/queues/foo/x/0",
			nil,
		)
		So(err, ShouldBeNil)
		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestHttpStat(t *testing.T) {
	Convey("Test Http Stat Api", t, func() {
		req, err := http.NewRequest(
			"GET",
			"http://127.0.0.1:8801/v1/admin/stat/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusOK)

		body, err := ioutil.ReadAll(resp.Body)
		So(err, ShouldBeNil)
		var qs queue.QueueStat
		err = json.Unmarshal(body, &qs)
		So(err, ShouldBeNil)
		So(qs.Name, ShouldEqual, "foo/x")
	})
}

func TestHttpEmpty(t *testing.T) {
	Convey("Test Http Empty Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8801/v1/admin/empty/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestHttpRemove(t *testing.T) {
	Convey("Test Http Remove Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8801/v1/admin/rm/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestCloseHttpEntry(t *testing.T) {
	Convey("Test Close Http Entry", t, func() {
		entrance.Stop()
		messageQueue = nil
		storage = nil
	})
}
