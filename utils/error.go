package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	// ErrNone is none error
	ErrNone = 100
	// ErrTopicNotExisted is topic not existed error
	ErrTopicNotExisted = 101
	// ErrLineNotExisted is line not existed error
	ErrLineNotExisted = 102
	// ErrNotDelivered is the message not delivered error
	ErrNotDelivered = 103
	// ErrBadKey is key bad error
	ErrBadKey = 104
	// ErrTopicExisted is topic has been existed error
	ErrTopicExisted = 105
	// ErrLineExisted is line has been existed error
	ErrLineExisted = 106
	// ErrBadRequest is bad request error
	ErrBadRequest = 400
	// ErrInternalError is internal error
	ErrInternalError = 500
)

var errorMap = map[int]string{
	// 404
	ErrNone:            "No Message",
	ErrTopicNotExisted: "Topic Not Existed",
	ErrLineNotExisted:  "Line Not Existed",
	ErrNotDelivered:    "Message Not Delivered",

	// 400
	ErrBadKey:       "Bad Key Format",
	ErrTopicExisted: "Topic Has Existed",
	ErrLineExisted:  "Line Has Existed",
	ErrBadRequest:   "Bad Client Request",

	// 500
	ErrInternalError: "Internal Error",
}

var errorStatus = map[int]int{
	ErrNone:            http.StatusNotFound,
	ErrTopicNotExisted: http.StatusNotFound,
	ErrLineNotExisted:  http.StatusNotFound,
	ErrNotDelivered:    http.StatusNotFound,
	ErrInternalError:   http.StatusInternalServerError,
}

// Error is the error type in uq
type Error struct {
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
	Cause     string `json:"cause,omitempty"`
}

// NewError returns a uq error
func NewError(errorCode int, cause string) *Error {
	return &Error{
		ErrorCode: errorCode,
		Message:   errorMap[errorCode],
		Cause:     cause,
	}
}

// Only for error interface
func (e Error) Error() string {
	return ItoaQuick(e.ErrorCode) + " " + e.Message + " (" + e.Cause + ")"
}

func (e Error) statusCode() int {
	status, ok := errorStatus[e.ErrorCode]
	if !ok {
		status = http.StatusBadRequest
	}
	return status
}

func (e Error) toJSONString() string {
	b, _ := json.Marshal(e)
	return string(b)
}

// WriteTo writes the error to a http response
func (e Error) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.statusCode())
	fmt.Fprintln(w, e.toJSONString())
}
