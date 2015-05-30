package queue

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Stat is the Stat of a UnitedQueue
type Stat struct {
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	Lines   []*Stat `json:"lines,omitempty"`
	Recycle string  `json:"recycle,omitempty"`
	Head    uint64  `json:"head"`
	IHead   uint64  `json:"ihead"`
	Tail    uint64  `json:"tail"`
	Count   uint64  `json:"count"`
}

// ToString returns the string of Stat
func (q *Stat) ToString() string {
	replys := q.ToStrings()
	reply := strings.Join(replys, "\r\n")
	return reply
}

// ToMcString returns the memcached string of Stat
func (q *Stat) ToMcString() string {
	replays := q.ToStrings()
	for i, replay := range replays {
		if replay != "" {
			replays[i] = "STAT " + replay
		}
	}
	return strings.Join(replays, "\r\n")
}

// ToStrings returns the strings of Stat
func (q *Stat) ToStrings() []string {
	var replys []string
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

// ToRedisStrings returns the redis strings of Stat
func (q *Stat) ToRedisStrings() []string {
	replys := q.ToStrings()
	replys = append(replys, "")
	return replys
}

// ToJSON returns the json string of Stat
func (q *Stat) ToJSON() ([]byte, error) {
	return json.Marshal(q)
}
