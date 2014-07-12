package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
)

const apiVersion = "DynamoDB_20120810"

var attempts = aws.AttemptStrategy{
	Min:   5,
	Total: 5 * time.Second,
	Delay: 200 * time.Millisecond,
}

// Specific error constants
var (
	ErrNotFound                        = errors.New("dynamodb: item not found")
	ErrFailedtoReadResponse            = errors.New("dynamodb: failed to read response")
	ErrAtLeastOneAttributeRequired     = errors.New("dynamodb: at least one attribute is required")
	ErrInconsistencyInTableDescription = errors.New("dynamodb: inconsistency found in TableDescriptionT")
	ErrNotImplemented                  = errors.New("dynamodb: Not implemented")
)

type UnexpectedResponseError struct {
	Response     []byte
	MarshalError error
}

func (e *UnexpectedResponseError) Error() string {
	return fmt.Sprintf("dynamodb: %s: unexpected response '%s'", e.MarshalError, e.Response)
}

// apiError represents an API error described at
// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html
type apiError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// Error represents an error in an operation with DynamoDB
type Error struct {
	// HTTP status code (200, 403, ...)
	StatusCode int
	// HTTP status line (400 Bad Request, ...)
	Status string
	// DynamoDB error code ("MalformedQueryString", ...)
	Code string
	// The human-oriented error message
	Message string
}

// UnmarshalJSON parses the JSON-encoded API error message data and
// stores the result in the value pointed by e.
func (e *Error) UnmarshalJSON(data []byte) error {
	ae := &apiError{}
	if err := json.Unmarshal(data, ae); err != nil {
		return err
	}
	e.Code = strings.SplitN(ae.Type, "#", 2)[1]
	e.Message = ae.Message
	return nil
}

func (e *Error) Error() string {
	return "dynamodb: " + e.Code + ": " + e.Message
}

func NewError(r *http.Response, jsonBody []byte) error {
	ddbError := &Error{
		StatusCode: r.StatusCode,
		Status:     r.Status,
	}
	if err := json.Unmarshal(jsonBody, ddbError); err != nil {
		return err
	}
	return ddbError
}

// Based on github.com/crowdmob/goamz/s3
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		return true
	}
	switch e := err.(type) {
	case *net.DNSError:
		return true
	case *net.OpError:
		switch e.Op {
		case "read", "write":
			return true
		}
	case *Error:
		switch e.Code {
		case "InternalError", "ProvisionedThroughputExceededException":
			return true
		}
		switch e.StatusCode {
		case 500, 503:
			return true
		}
	}
	return false
}

func target(name string) string {
	return apiVersion + "." + name
}
