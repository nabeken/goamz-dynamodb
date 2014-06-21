package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/crowdmob/goamz/aws"
)

type Server struct {
	Auth   aws.Auth
	Region aws.Region
}

// Specific error constants
var (
	ErrNotFound                         = errors.New("dynamodb: item not found")
	ErrAtLeastOneAttributeRequired      = errors.New("dynamodb: at least one attribute is required")
	ErrInconsistencyInTableDescriptionT = errors.New("dynamodb: inconsistency found in TableDescriptionT")
)

type UnexpectedResponseError struct {
	Response []byte
}

func (e *UnexpectedResponseError) Error() string {
	return fmt.Sprintf("dynamodb: unexpected response '%s'", e.Response)
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

func (s *Server) queryServer(target string, query *Query) ([]byte, error) {
	data := strings.NewReader(query.String())
	hreq, err := http.NewRequest("POST", s.Region.DynamoDBEndpoint+"/", data)
	if err != nil {
		return nil, err
	}

	hreq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	hreq.Header.Set("X-Amz-Date", time.Now().UTC().Format(aws.ISO8601BasicFormat))
	hreq.Header.Set("X-Amz-Target", target)

	token := s.Auth.Token()
	if token != "" {
		hreq.Header.Set("X-Amz-Security-Token", token)
	}

	signer := aws.NewV4Signer(s.Auth, "dynamodb", s.Region)
	signer.Sign(hreq)

	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Could not read response body")
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, NewError(resp, body)
	}
	return body, nil
}

func target(name string) string {
	return "DynamoDB_20120810." + name
}
