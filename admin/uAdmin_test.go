package admin

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
	adminServer  Administrator
	client       *http.Client
)

func init() {
	client = new(http.Client)
}

func TestNewAdmin(t *testing.T) {
	Convey("Test New Admin", t, func() {
		var err error
		storage, err = store.NewMemStore()
		So(err, ShouldBeNil)
		So(storage, ShouldNotBeNil)
		messageQueue, err = queue.NewUnitedQueue(storage, "127.0.0.1", 8800, nil, "uq")
		So(err, ShouldBeNil)
		So(messageQueue, ShouldNotBeNil)

		adminServer, err = NewUnitedAdmin("0.0.0.0", 8800, messageQueue)
		So(err, ShouldBeNil)
		So(adminServer, ShouldNotBeNil)

		go func() {
			adminServer.ListenAndServe()
		}()
	})
}

func TestAdminAdd(t *testing.T) {
	Convey("Test Admin Add Api", t, func() {
		bf := bytes.NewBufferString("topic=foo")
		body := ioutil.NopCloser(bf)
		req, err := http.NewRequest(
			"PUT",
			"http://127.0.0.1:8800/v1/queues",
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
			"http://127.0.0.1:8800/v1/queues",
			body,
		)
		So(err, ShouldBeNil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err = client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusCreated)
	})
}

func TestAdminPush(t *testing.T) {
	Convey("Test Admin Push Api", t, func() {
		bf := bytes.NewBufferString("value=1")
		body := ioutil.NopCloser(bf)
		req, err := http.NewRequest(
			"POST",
			"http://127.0.0.1:8800/v1/queues/foo",
			body,
		)
		So(err, ShouldBeNil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestAdminPop(t *testing.T) {
	Convey("Test Admin Pop Api", t, func() {
		req, err := http.NewRequest(
			"GET",
			"http://127.0.0.1:8800/v1/queues/foo/x",
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

func TestAdminConfirm(t *testing.T) {
	Convey("Test Admin Confirm Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8800/v1/queues/foo/x/0",
			nil,
		)
		So(err, ShouldBeNil)
		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestAdminStat(t *testing.T) {
	Convey("Test Admin Stat Api", t, func() {
		req, err := http.NewRequest(
			"GET",
			"http://127.0.0.1:8800/v1/admin/stat/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusOK)

		body, err := ioutil.ReadAll(resp.Body)
		So(err, ShouldBeNil)
		var qs queue.Stat
		err = json.Unmarshal(body, &qs)
		So(err, ShouldBeNil)
		So(qs.Name, ShouldEqual, "foo/x")
	})
}

func TestAdminEmpty(t *testing.T) {
	Convey("Test Admin Empty Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8800/v1/admin/empty/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestAdminRemove(t *testing.T) {
	Convey("Test Admin Remove Api", t, func() {
		req, err := http.NewRequest(
			"DELETE",
			"http://127.0.0.1:8800/v1/admin/rm/foo/x",
			nil,
		)
		So(err, ShouldBeNil)

		resp, err := client.Do(req)
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, http.StatusNoContent)
	})
}

func TestCloseAdmin(t *testing.T) {
	Convey("Test Close Admin", t, func() {
		adminServer.Stop()
		messageQueue.Close()
		messageQueue = nil
		storage = nil
	})
}
