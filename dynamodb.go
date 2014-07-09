package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/crowdmob/goamz/aws"
)

const apiVersion = "DynamoDB_20120810"

var attempts = aws.AttemptStrategy{
	Min:   5,
	Total: 5 * time.Second,
	Delay: 200 * time.Millisecond,
}

type Server struct {
	Auth   aws.Auth
	Region aws.Region
}

func (s *Server) NewTable(name string, key PrimaryKey) *Table {
	return &Table{s, name, key}
}

func (s *Server) ListTables() ([]string, error) {
	var tables []string

	query := ListTablesQuery{}

	jsonResponse, err := s.queryServer(target("ListTables"), query)

	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	response, err := json.Get("TableNames").Array()

	if err != nil {
		return nil, &UnexpectedResponseError{jsonResponse}
	}

	for _, value := range response {
		if t, ok := (value).(string); ok {
			tables = append(tables, t)
		}
	}

	return tables, nil
}

func (s *Server) UpdateTable(query UpdateTableQuery) (string, error) {
	jsonResponse, err := s.queryServer(target("UpdateTable"), query)
	if err != nil {
		return "", err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return "", err
	}
	return json.Get("TableDescription").Get("TableStatus").MustString(), nil
}

func (s *Server) CreateTable(tableDescription TableDescription) (string, error) {
	query := CreateTableQuery{
		AttributeDefinitions:  tableDescription.AttributeDefinitions,
		KeySchema:             tableDescription.KeySchema,
		ProvisionedThroughput: tableDescription.ProvisionedThroughput,
		TableName:             tableDescription.TableName,
	}
	if len(tableDescription.GlobalSecondaryIndexes) > 0 {
		query.GlobalSecondaryIndexes = tableDescription.GlobalSecondaryIndexes
	}
	if len(tableDescription.LocalSecondaryIndexes) > 0 {
		query.LocalSecondaryIndexes = tableDescription.LocalSecondaryIndexes
	}

	jsonResponse, err := s.queryServer(target("CreateTable"), query)
	if err != nil {
		return "", err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return "", err
	}

	return json.Get("TableDescription").Get("TableStatus").MustString(), nil
}

func (s *Server) DeleteTable(tableDescription TableDescription) (string, error) {
	query := DeleteTableQuery{
		TableName: tableDescription.TableName,
	}

	jsonResponse, err := s.queryServer(target("DeleteTable"), query)
	if err != nil {
		return "", err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return "", err
	}

	return json.Get("TableDescription").Get("TableStatus").MustString(), nil
}

func (s *Server) DescribeTable(name string) (*TableDescription, error) {
	q := DescribeTableQuery{
		TableName: name,
	}

	jsonResponse, err := s.queryServer(target("DescribeTable"), q)
	if err != nil {
		return nil, err
	}

	var r describeTableResponse
	err = json.Unmarshal(jsonResponse, &r)
	if err != nil {
		return nil, err
	}

	return &r.Table, nil
}

// Specific error constants
var (
	ErrNotFound                        = errors.New("dynamodb: item not found")
	ErrFailedtoReadResponse            = errors.New("dynamodb: failed to read response")
	ErrAtLeastOneAttributeRequired     = errors.New("dynamodb: at least one attribute is required")
	ErrInconsistencyInTableDescription = errors.New("dynamodb: inconsistency found in TableDescriptionT")
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

func (s *Server) queryServer(target string, query interface{}) ([]byte, error) {
	j, jerr := json.Marshal(query)
	if jerr != nil {
		return nil, jerr
	}
	hreq, err := http.NewRequest("POST", s.Region.DynamoDBEndpoint+"/", bytes.NewReader(j))
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

	for attempt := attempts.Start(); attempt.Next(); {
		resp, err := http.DefaultClient.Do(hreq)

		if err != nil {
			if shouldRetry(err) {
				continue
			}
			return nil, err
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, ErrFailedtoReadResponse
		}
		if resp.StatusCode != 200 {
			err = NewError(resp, body)
			if shouldRetry(err) {
				continue
			}
			return nil, err
		}
		return body, nil
	}
	return nil, err
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
