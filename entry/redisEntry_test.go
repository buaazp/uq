package entry

import (
	"testing"
	"time"

	"github.com/buaazp/uq/queue"
	"github.com/buaazp/uq/store"
	"github.com/garyburd/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"
)

var conn redis.Conn

func TestNewRedisEntry(t *testing.T) {
	Convey("Test New Redis Entry", t, func() {
		var err error
		storage, err = store.NewMemStore()
		So(err, ShouldBeNil)
		So(storage, ShouldNotBeNil)
		messageQueue, err = queue.NewUnitedQueue(storage, "127.0.0.1", 8803, nil, "uq")
		So(err, ShouldBeNil)
		So(messageQueue, ShouldNotBeNil)

		entrance, err = NewRedisEntry("0.0.0.0", 8803, messageQueue)
		So(err, ShouldBeNil)
		So(entrance, ShouldNotBeNil)

		go func() {
			entrance.ListenAndServe()
		}()
	})
}

func TestRedisAdd(t *testing.T) {
	Convey("Test Redis Add Api", t, func() {
		conn, _ = redis.DialTimeout("tcp", "127.0.0.1:8803", 0, 1*time.Second, 1*time.Second)
		So(conn, ShouldNotBeNil)

		_, err := conn.Do("QADD", "foo")
		So(err, ShouldBeNil)

		_, err = conn.Do("QADD", "foo/x", "10s")
		So(err, ShouldBeNil)
	})
}

func TestRedisPush(t *testing.T) {
	Convey("Test Redis Push Api", t, func() {
		_, err := conn.Do("QPUSH", "foo", "1")
		So(err, ShouldBeNil)
	})
}

func TestRedisPop(t *testing.T) {
	Convey("Test Redis Pop Api", t, func() {
		rpl, err := redis.Values(conn.Do("QPOP", "foo/x"))
		So(err, ShouldBeNil)
		v, err := redis.String(rpl[0], err)
		So(err, ShouldBeNil)
		So(v, ShouldEqual, "1")
		id, err := redis.String(rpl[1], err)
		So(err, ShouldBeNil)
		So(id, ShouldEqual, "foo/x/0")
	})
}

func TestRedisConfirm(t *testing.T) {
	Convey("Test Redis Confirm Api", t, func() {
		_, err := conn.Do("QDEL", "foo/x/0")
		So(err, ShouldBeNil)
	})
}

func TestCloseRedisEntry(t *testing.T) {
	Convey("Test Close Redis Entry", t, func() {
		entrance.Stop()
		messageQueue = nil
		storage = nil
	})
}
