package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/buaazp/uq/entry"
)

func encode(qr *entry.HttpQueueRequest) ([]byte, error) {
	jsonData, err := json.Marshal(qr)
	if err != nil {
		return nil, err
	}
	fmt.Printf("encode succ: len = %d json: %s\n", len(jsonData), string(jsonData))
	return jsonData, nil
}

func decode(data []byte) error {
	qr := new(entry.HttpQueueRequest)
	err := json.Unmarshal(data, qr)
	if err != nil {
		return err
	}
	if qr.Recycle != "" {
		recycle, err := time.ParseDuration(qr.Recycle)
		if err != nil {
			return err
		}
		fmt.Printf("decode succ: %s\n", recycle.String())
	} else {
		fmt.Printf("decode succ: %v\n", qr)
	}

	return nil
}

func main() {
	qr := new(entry.HttpQueueRequest)
	qr.TopicName = "foo"
	qr.LineName = "x"
	qr.Recycle = "1h10m30s"

	jsonData, err := encode(qr)
	if err != nil {
		fmt.Printf("encode failed: %s\n", err)
	}

	err = decode(jsonData)
	if err != nil {
		fmt.Printf("decode failed: %s\n", err)
	}
	return
}
