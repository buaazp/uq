package entry

import (
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	. "github.com/smartystreets/goconvey/convey"
)

var mc *memcache.Client

func init() {
	mc = memcache.New("localhost:8802")
}

func TestNewMcEntry(t *testing.T) {
	Convey("Test New Mc Entry", t, func() {
		var err error
		storage, err = store.NewMemStore()
		So(err, ShouldBeNil)
		So(storage, ShouldNotBeNil)
		messageQueue, err = queue.NewUnitedQueue(storage, "127.0.0.1", 8802, nil, "uq")
		So(err, ShouldBeNil)
		So(messageQueue, ShouldNotBeNil)

		entrance, err = NewMcEntry("0.0.0.0", 8802, messageQueue)
		So(err, ShouldBeNil)
		So(entrance, ShouldNotBeNil)

		go func() {
			entrance.ListenAndServe()
		}()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestMcAdd(t *testing.T) {
	Convey("Test Mc Add Api", t, func() {
		err := mc.Add(&memcache.Item{
			Key:   "foo",
			Value: []byte{},
		})
		So(err, ShouldBeNil)

		err = mc.Add(&memcache.Item{
			Key:   "foo/x",
			Value: []byte("10s"),
		})
		So(err, ShouldBeNil)
	})
}

func TestMcPush(t *testing.T) {
	Convey("Test Mc Push Api", t, func() {
		err := mc.Set(&memcache.Item{
			Key:   "foo",
			Value: []byte("1"),
		})
		So(err, ShouldBeNil)
	})
}

func TestMcPop(t *testing.T) {
	Convey("Test Mc Pop Api", t, func() {
		it, err := mc.Get("foo/x")
		So(err, ShouldBeNil)
		v := string(it.Value)
		So(v, ShouldEqual, "1")
	})
}

func TestMcConfirm(t *testing.T) {
	Convey("Test Mc Confirm Api", t, func() {
		err := mc.Delete("foo/x/0")
		So(err, ShouldBeNil)
	})
}

func TestCloseMcEntry(t *testing.T) {
	Convey("Test Close Mc Entry", t, func() {
		entrance.Stop()
		messageQueue = nil
		storage = nil
	})
}
