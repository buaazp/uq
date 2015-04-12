package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	ErrNone            = 100
	ErrTopicNotExisted = 101
	ErrLineNotExisted  = 102
	ErrNotDelivered    = 103
	ErrBadKey          = 104
	ErrTopicExisted    = 105
	ErrLineExisted     = 106
	ErrBadRequest      = 400
	ErrInternalError   = 500
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

type Error struct {
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
	Cause     string `json:"cause,omitempty"`
}

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

func (e Error) toJsonString() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func (e Error) statusCode() int {
	status, ok := errorStatus[e.ErrorCode]
	if !ok {
		status = http.StatusBadRequest
	}
	return status
}

func (e Error) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.statusCode())
	fmt.Fprintln(w, e.toJsonString())
}
