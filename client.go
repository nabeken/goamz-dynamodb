package dynamodb

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/crowdmob/goamz/aws"
)

type Client struct {
	Auth   aws.Auth
	Region aws.Region
}

func (c *Client) BatchGetItem(r *BatchGetItemRequest) (*BatchGetItemResult, error) {
	ret := &BatchGetItemResult{}
	err := c.Do(target("BatchGetItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) BatchWriteItem(r *BatchWriteItemRequest) (*BatchWriteItemResult, error) {
	ret := &BatchWriteItemResult{}
	err := c.Do(target("BatchWriteItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) CreateTable(r *CreateTableRequest) (*CreateTableResult, error) {
	ret := &CreateTableResult{}
	err := c.Do(target("CreateTable"), r).Scan(ret)
	return ret, err
}

func (c *Client) DeleteItem(r *DeleteItemRequest) (*DeleteItemResult, error) {
	ret := &DeleteItemResult{}
	err := c.Do(target("DeleteItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) DeleteTable(r *DeleteTableRequest) (*DeleteTableResult, error) {
	ret := &DeleteTableResult{}
	err := c.Do(target("DeleteTable"), r).Scan(ret)
	return ret, err
}

func (c *Client) DescribeTable(r *DescribeTableRequest) (*DescribeTableResult, error) {
	ret := &DescribeTableResult{}
	err := c.Do(target("DescribeTable"), r).Scan(ret)
	return ret, err
}

func (c *Client) GetItem(r *GetItemRequest) (*GetItemResult, error) {
	ret := &GetItemResult{}
	err := c.Do(target("GetItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) ListTables(r *ListTablesRequest) (*ListTablesResult, error) {
	// TODO(nabeken): Add paging support
	ret := &ListTablesResult{}
	err := c.Do(target("ListTables"), r).Scan(ret)
	return ret, err
}

func (c *Client) PutItem(r *PutItemRequest) (*PutItemResult, error) {
	ret := &PutItemResult{}
	err := c.Do(target("PutItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) Query(r *QueryRequest) (*QueryResult, error) {
	ret := &QueryResult{}
	err := c.Do(target("Query"), r).Scan(ret)
	return ret, err
}

func (c *Client) Scan(r *ScanRequest) (*ScanResult, error) {
	// TODO(nabeken): Add paging support
	ret := &ScanResult{}
	err := c.Do(target("Scan"), r).Scan(ret)
	return ret, err
}

func (c *Client) UpdateItem(r *UpdateItemRequest) (*UpdateItemResult, error) {
	ret := &UpdateItemResult{}
	err := c.Do(target("UpdateItem"), r).Scan(ret)
	return ret, err
}

func (c *Client) UpdateTable(r *UpdateTableRequest) (*UpdateTableResult, error) {
	ret := &UpdateTableResult{}
	err := c.Do(target("UpdateTable"), r).Scan(ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (c *Client) Do(target string, query interface{}) *response {
	j, jerr := json.Marshal(query)
	if jerr != nil {
		return &response{nil, jerr}
	}
	hreq, err := http.NewRequest("POST", c.Region.DynamoDBEndpoint+"/", bytes.NewReader(j))
	if err != nil {
		return &response{nil, err}
	}

	hreq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	hreq.Header.Set("X-Amz-Date", time.Now().UTC().Format(aws.ISO8601BasicFormat))
	hreq.Header.Set("X-Amz-Target", target)

	token := c.Auth.Token()
	if token != "" {
		hreq.Header.Set("X-Amz-Security-Token", token)
	}

	signer := aws.NewV4Signer(c.Auth, "dynamodb", c.Region)
	signer.Sign(hreq)

	for attempt := attempts.Start(); attempt.Next(); {
		resp, err := http.DefaultClient.Do(hreq)

		if err != nil {
			if shouldRetry(err) {
				continue
			}
			return &response{nil, err}
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return &response{nil, ErrFailedtoReadResponse}
		}
		if resp.StatusCode != 200 {
			err = NewError(resp, body)
			if shouldRetry(err) {
				continue
			}
			return &response{nil, err}
		}
		return &response{body, nil}
	}
	return &response{nil, err}
}

type response struct {
	json []byte
	err  error
}

func (r *response) Scan(result interface{}) error {
	if r.err != nil {
		return r.err
	}
	jerr := json.Unmarshal(r.json, result)
	if jerr != nil {
		return &UnexpectedResponseError{r.json, jerr}
	}
	return nil
}
