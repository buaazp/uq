package queue

import (
	"encoding/json"
	"strconv"
	"strings"
)

type QueueStat struct {
	Name    string       `json:"name"`
	Type    string       `json:"type"`
	Lines   []*QueueStat `json:"lines,omitempty"`
	Recycle string       `json:"recycle,omitempty"`
	Head    uint64       `json:"head"`
	IHead   uint64       `json:"ihead"`
	Tail    uint64       `json:"tail"`
	Count   uint64       `json:"count"`
}

func (q *QueueStat) ToString() string {
	replys := q.ToStrings()
	reply := strings.Join(replys, "\r\n")
	return reply
}

func (q *QueueStat) ToMcString() string {
	replays := q.ToStrings()
	for i, replay := range replays {
		if replay != "" {
			replays[i] = "STAT " + replay
		}
	}
	return strings.Join(replays, "\r\n")
}

func (q *QueueStat) ToStrings() []string {
	replys := make([]string, 0)
	replys = append(replys, "name:"+q.Name)
	if q.Type == "line" {
		replys = append(replys, "recycle:"+q.Recycle)
	}

	replys = append(replys, "head:"+strconv.FormatUint(q.Head, 10))
	if q.Type == "line" {
		replys = append(replys, "ihead:"+strconv.FormatUint(q.IHead, 10))
	}
	replys = append(replys, "tail:"+strconv.FormatUint(q.Tail, 10))
	replys = append(replys, "count:"+strconv.FormatUint(q.Count, 10))

	if q.Type == "topic" && q.Lines != nil {
		for _, lineStat := range q.Lines {
			replys = append(replys, "")
			ls := lineStat.ToStrings()
			replys = append(replys, ls...)
		}
	}

	return replys
}

func (q *QueueStat) ToRedisStrings() []string {
	replys := q.ToStrings()
	replys = append(replys, "")
	return replys
}

func (q *QueueStat) ToJson() ([]byte, error) {
	return json.Marshal(q)
}
