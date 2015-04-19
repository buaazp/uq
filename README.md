## United Queue

Uq is another distributed message queue. It is written in Go and has many useful features including:

- [multi client APIs support](#client-api)
- [consume messages from multi lines of one topic](#topic-and-line)
- [message confirmation and recycle](#message-confirmation-and-recycle)
- [distributed cluster](#distributed-cluster)
- [RESTful admin methods](#admin-api)
- [multi ways of message persistence](#message-persistence)

Messages in uq is very safe. It exists until a consumer confirms clearly. Consumer lines of a topic are independent. So one independent workflow can use a line to consume the messages in topic. A group of uq can make up a cluster. Cluster information is stored in [etcd](https://github.com/coreos/etcd). Clients can get the information from etcd and then connect to one or some uq servers in the cluster. Or you can use [libuq](https://github.com/buaazp/libuq) to communicate with uq cluster.

Author: [@招牌疯子](http://weibo.com/819880808)  
Contact me: zp@buaa.us


### Getting Started

Get uq with go get like below:

```
go get -u github.com/buaazp/uq
uq -h
Usage of ./uq:
  -admin-port=8809: admin listen port
  -cluster=“uq”: cluster name in etcd
  -db=“goleveldb”: backend storage type [goleveldb/memdb]
  -dir=“./data”: backend storage path
  -etcd=“”: etcd service location
  -host=“0.0.0.0”: listen ip
  -ip=“127.0.0.1”: self ip/host address
  -log=“”: uq log path
  -port=8808: listen port
  -pprof-port=8080: pprof listen port
  -protocol=“redis”: frontend interface type [redis/mc/http]
```

### Concepts in UQ

#### topic and line

A topic is a class to store messages. Producers push messages into a topic. A line is a class to output messages for a clear purpose. A topic can have many lines for different purposes. For example:

1. Users upload pictures to [weibo.com](http://weibo.com). All the upload messages pushed into a topic named `wb_img_upload`.
2. Compress service pops the message from a line named `img_to_compress`, then compresses the picture and confirms the message in this line.
3. Analysis service pops the message from a line named `img_to_analysis`, then analyzes the picture and confirm the message in this line.
4. Any other services can pop and confirm messages from its own line. The status of different lines are independent.

#### message confirmation and recycle

Messages popped from a line should be confirmed after disposing. If a consumer pops a message but fails to dispose it, this message will be pushed back into the line after a recycle time which is set when creating the line.

If a line is created with no recycle time. The line will degrade to a classical message queue, which means if a message is popped, it is lost.

#### queue methods

Uq defines a list of queue methods:

- add tname = create a topic
- add tname/lname 10s = create a line with the recycle time
- push tname value = push a message into the topic
- pop tname/lname = pop the latest message of the line
- del tname/lname/mID = confirm the message according to the message ID

Different protocols implement the queue methods above in its own way. But they are similar.

#### admin methods

And uq has some admin methods to manage the queue:

- stat tname = get the topic’s status
- stat tname/lname = get the line’s status
- empty tname/lname = empty all the messages in a line
- empty tname = empty all the messages in the topic and its lines
- rm tname/lname = remove a line from the topic
- rm tname = remove all lines of the topic and itself

### Client API

Uq supports many client APIs like memcached, redis and http RESTful api. Choose the protocol you are most familiar with.

#### memcached api

Uq is designed to be an alternative to [memcacheQ](http://memcachedb.org/memcacheq/). And the author of mcq [@stvchu](http://stvchu.github.io/) provides a lot of advice for designing uq. Thank him a lot for his help.

Start uq with mc protocol and connect uq with telnet:

```
uq -protocol mc
telnet 127.0.0.1 8808
```

Send memcached requests like these:

```
// create a topic
add foo 0 0 0

STORED

// create a line with 10s recycle time
add foo/x 0 0 3
10s
STORED

// push a message into the topic
set foo 0 0 3
bar
STORED

// pop a message from the line
get foo/x id
VALUE foo/x 0 3
bar
VALUE id 0 7
foo/x/0
END

// confirm a message
delete foo/x/0
DELETED

```

#### redis api

Uq also supports redis protocol. And using redis protocol is easier than memcached. Start uq with redis protocol and use redis-cli to connect:

```
uq -protocol redis
redis-cli -p 8808
```

Send redis requests like these:

```
// create a topic
127.0.0.1:8808> add foo
OK

// create a line with 10s recycle time
127.0.0.1:8808> add foo/x 10s
OK

// push a message into the topic
127.0.0.1:8808> set foo bar
OK

// pop a message from the line
127.0.0.1:8808> get foo/x
1) “bar”
2) “foo/x/0”

// confirm a message
127.0.0.1:8808> del foo/x/0
OK

```

#### http RESTful api

If you don’t like to use any of memcached or redis client library, you can use http RESTful api which is simple and lightweight. Start uq with http protocol:

```
uq -protocol http
```

Send http requests like these:

```
// create a topic
curl -XPUT -i localhost:8808/v1/queues -d topic=foo
HTTP/1.1 201 Created
Date: Sat, 18 Apr 2015 09:17:31 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8

// create a line with 10s recycle time
curl -XPUT -i localhost:8808/v1/queues -d “topic=foo&line=x&recycle=10s”
HTTP/1.1 201 Created
Date: Sat, 18 Apr 2015 09:17:57 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8

// push a message into the topic
curl -XPOST -i localhost:8808/v1/queues/foo -d “value=bar”
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 09:18:28 GMT

// pop a message from the line
curl -i localhost:8808/v1/queues/foo/x
HTTP/1.1 200 OK
Content-Type: text/plain
X-Uq-Id: foo/x/0
Date: Sat, 18 Apr 2015 09:16:38 GMT
Content-Length: 3

bar

// confirm a message
curl -XDELETE -i localhost:8808/v1/queues/foo/x/0
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 09:19:08 GMT

```

#### admin api

An admin http server starts when uq is started. It uses another port (default is 8809) to listen for http requests.

```
// get stat of a line
curl -i localhost:8809/v1/admin/stat/foo/x
HTTP/1.1 200 OK
Content-Type: application/json
Date: Sat, 18 Apr 2015 10:55:03 GMT
Content-Length: 84

{“name”:”foo/x”,”type”:”line”,”recycle”:”10s”,”head”:1,”ihead”:1,”tail”:2,”count”:1}

// get stat of a topic
curl -i localhost:8809/v1/admin/stat/foo
HTTP/1.1 200 OK
Content-Type: application/json
Date: Sat, 18 Apr 2015 10:55:42 GMT
Content-Length: 162

{“name”:”foo”,”type”:”topic”,”lines”:[{“name”:”foo/x”,”type”:”line”,”recycle”:”10s”,”head”:1,”ihead”:1,”tail”:2,”count”:1}],”head”:1,”ihead”:0,”tail”:2,”count”:1}

// empty a line
curl -XDELETE -i localhost:8809/v1/admin/empty/foo/x
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 10:56:42 GMT

// empty a topic
curl -XDELETE -i localhost:8809/v1/admin/empty/foo
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 10:57:02 GMT

// remove a line
curl -XDELETE -i localhost:8809/v1/admin/rm/foo/x
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 10:57:20 GMT

// remove a topic
curl -XDELETE -i localhost:8809/v1/admin/rm/foo
HTTP/1.1 204 No Content
Date: Sat, 18 Apr 2015 10:57:33 GMT

```

STAT method is also supported in memcached and redis protocol:

```
// in memcached protocol
stats foo
STAT name:foo
STAT head:0
STAT tail:2
STAT count:2

STAT name:foo/x
STAT recycle:0
STAT head:1
STAT ihead:0
STAT tail:2
STAT count:1

// in redis protocol
127.0.0.1:8808> info foo
name:foo
head:1
tail:2
count:1

name:foo/x
recycle:0
head:1
ihead:0
tail:2
count:1
```

#### api compatibility

The compatibility of different protocols can be found below:

| Method | Redis | Mc | Http | Description |
| :----: |:---:|:---:|:---:|:---|
| add | √ | √ | √ | create a topic/line |
| push | √ | √ | √ | push a message into the topic |
| pop | √ | √ | √ | pop the latest message of the line |
| del | √ | √ | √ | confirm the message according to the message ID |
| stat | √ | √ | √ | get the topic’s/line’s status |
| empty | √ | × | √ | empty all the messages in a topic/line |
| rm | × | × | √ | remove a topic/line |

### Distributed Cluster

Uq cluster is based on etcd. You need to install and start etcd first. Then set etcd servers’ url when starting uq instances.

```
// start instance 1
uq -port 8708 -admin-port 8709 -dir ./uq1 -etcd http://localhost:4001 -cluster uq
// start instance 2
uq -port 8808 -admin-port 8809 -dir ./uq2 -etcd http://localhost:4001 -cluster uq
// start instance 3
uq -port 8908 -admin-port 8909 -dir ./uq3 -etcd http://localhost:4001 -cluster uq
```

Then these uq instances make up a uq cluster. All instances in a cluster have same topics and lines. But the message in them is independent. In other words, a message can be only pushed into one instance in a cluster. Queue workflow in uq cluster is like below:

1. Client A adds topic [foo] in instance 1. All instances has topic named [foo].
2. Client B adds line [foo/x] in instance 2. All instances has line [foo/x].
3. Client C pushes a message [bar] to instacne 3. C can pop this message from instance 3.
4. A and B cannot pop any message from instance 1/2 because they have no message.
5. A, B, C continually push messages to topic [foo] in the instace they connect to.
6. Consumer D can pop [foo/x] to get a message from any instance in the cluster. All the messages in different instances are belong to line [foo/x].
7. Consumer can only confirm a message in the instance which popped the message.

#### using libuq

Maybe you are in trouble with using the api of etcd and consideration of the connection pool. You can use [libuq](https://github.com/buaazp/libuq) to write simple codes. Libuq is designed for uq cluster. Now only Golang is supported. You can find more information about libuq in its github repository.

### Message Persistence

The default storage of uq is goleveldb. It stores all the data in disk. So the messages are persistent. If the uq server broken down, the queue will recover after uq restarts.

If you need a faster uq, you can use memory to store the messages. But if uq is shut down, the messages will be lost.

Other storage like rocksdb, leveldb will be supported in the future.

### Unit Test

Uq’s main funtions in package amdin/entry/queue/store/utils have been tested. You can test it by yourself after installing goconvey:

```
go get github.com/smartystreets/goconvey
cd $GOPATH/src/github.com/buaazp/uq
goconvey
```

![Unit Tests Result](http://ww4.sinaimg.cn/large/4c422e03jw1era17icm96j212q0oowl3.jpg)

### Benchmark Test

This benchmark test is between uq and memcacheQ v0.2.0 in my 13" Macbook Pro early 2011 with SSD. Uq is started with:

```
./uq -protocol mc
```

MemcacheQ is started with:

```
memcacheq -p 8808 -r -c 1024 -H ./mcq_data/ -N -R -L 1024 -A 65536 -T 15 -B 8192 -m 256 -E 4096
```

The test tool is `test/mc_bench.go`. Build and start with:

```
go build mc_bench.go
./mc_bench -c 10 -n 500000 -m push
```

The result of benchmark test:

|  Program  |       QPS      | Throughput |
| :-------- |:--------------:| ----------:|
| memcacheQ | 4169.952 msg/s | 0.795 MB/s |
| UQ        | 4319.417 msg/s | 0.824 MB/s |

Uq performs as well as memcachedQ now. With the snappy compression of goleveldb, uq's data size is only 12MB while memcachedQ generated 4.5GB data after this test. You can use NoCompression option for goleveldb if you want.

Anyway, we need more optimization measures to improve performance of uq.

### Feedback:

If you have any question, please submit comment in my [BLOG](http://blog.buaa.us/) or mention me on [Weibo](http://weibo.com/819880808), [twitter](https://twitter.com/buaazp).
Technical issues are also welcome to be submitted on [GitHub Issues](https://github.com/buaazp/uq/issues).

