package dynamodb

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/crowdmob/goamz/aws"
)

/*
http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html
List of actions as of API version 2012-08-10
	BatchGetItem
	BatchWriteItem
	CreateTable
	DeleteItem
	DeleteTable
	DescribeTable
	GetItem
	ListTables
	PutItem
	Query
	Scan
	UpdateItem
	UpdateTable
*/

type Client struct {
	Auth       aws.Auth
	Region     aws.Region
	HTTPClient http.Client
}

func (c *Client) BatchGetItem(items map[string]KeysAndAttributes, bopt *BatchGetItemOption) (*BatchGetItemResult, error) {
	ret := &BatchGetItemResult{}
	err := c.Do(&RawRequest{"BatchGetItem", struct {
		RequestItems map[string]KeysAndAttributes
		*BatchGetItemOption
	}{
		items,
		bopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) BatchWriteItem(items map[string][]WriteRequest, bopt *BatchWriteItemOption) (*BatchWriteItemResult, error) {
	ret := &BatchWriteItemResult{}
	err := c.Do(&RawRequest{"BatchWriteItem", struct {
		RequestItems map[string][]WriteRequest
		*BatchWriteItemOption
	}{
		items,
		bopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) CreateTable(t *Table, topt *TableOption) (*CreateTableResult, error) {
	ret := &CreateTableResult{}
	err := c.Do(&RawRequest{"CreateTable", struct {
		*Table
		*TableOption
	}{
		t,
		topt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) DeleteItem(table string, key map[string]AttributeValue, dopt *DeleteItemOption) (*DeleteItemResult, error) {
	ret := &DeleteItemResult{}
	err := c.Do(&RawRequest{"DeleteItem", struct {
		TableName string
		Key       map[string]AttributeValue
		*DeleteItemOption
	}{
		table,
		key,
		dopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) DeleteTable(table string) (*DeleteTableResult, error) {
	ret := &DeleteTableResult{}
	err := c.Do(&RawRequest{"DeleteTable", struct {
		TableName string
	}{
		table,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) DescribeTable(table string) (*DescribeTableResult, error) {
	ret := &DescribeTableResult{}
	err := c.Do(&RawRequest{"DescribeTable", struct {
		TableName string
	}{
		table,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) GetItem(table string, key map[string]AttributeValue, gopt *GetItemOption) (*GetItemResult, error) {
	ret := &GetItemResult{}
	err := c.Do(&RawRequest{"GetItem", struct {
		TableName string
		Key       map[string]AttributeValue
		*GetItemOption
	}{
		table,
		key,
		gopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) ListTables(lopt *ListTablesOption) (*ListTablesResult, error) {
	ret := &ListTablesResult{}
	err := c.Do(&RawRequest{"ListTables", struct {
		*ListTablesOption
	}{
		lopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) PutItem(table string, item Item, popt *PutItemOption) (*PutItemResult, error) {
	ret := &PutItemResult{}
	err := c.Do(&RawRequest{"PutItem", struct {
		TableName string
		Item      Item
		*PutItemOption
	}{
		table,
		item,
		popt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) Query(table string, conditions *KeyConditions, qopt *QueryOption) (*QueryResult, error) {
	ret := &QueryResult{}
	err := c.Do(&RawRequest{"Query", struct {
		TableName     string
		KeyConditions *KeyConditions
		*QueryOption
	}{
		table,
		conditions,
		qopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) Scan(table string, sopt *ScanOption) (*ScanResult, error) {
	ret := &ScanResult{}
	err := c.Do(&RawRequest{"Scan", struct {
		TableName string
		*ScanOption
	}{
		table,
		sopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) UpdateItem(table string, key map[string]AttributeValue, uopt *UpdateItemOption) (*UpdateItemResult, error) {
	ret := &UpdateItemResult{}
	err := c.Do(&RawRequest{"UpdateItem", struct {
		TableName string
		Key       map[string]AttributeValue
		*UpdateItemOption
	}{
		table,
		key,
		uopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) UpdateTable(table string, uopt *UpdateTableOption) (*UpdateTableResult, error) {
	ret := &UpdateTableResult{}
	err := c.Do(&RawRequest{"UpdateTable", struct {
		TableName string
		*UpdateTableOption
	}{
		table,
		uopt,
	}}).Scan(ret)
	return ret, err
}

func (c *Client) Do(req *RawRequest) *Response {
	j, jerr := json.Marshal(req.Param)
	if jerr != nil {
		return &Response{jerr, nil}
	}
	hreq, err := http.NewRequest("POST", c.Region.DynamoDBEndpoint+"/", bytes.NewReader(j))
	if err != nil {
		return &Response{err, nil}
	}

	hreq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	hreq.Header.Set("X-Amz-Date", time.Now().UTC().Format(aws.ISO8601BasicFormat))
	hreq.Header.Set("X-Amz-Target", target(req.Target))

	token := c.Auth.Token()
	if token != "" {
		hreq.Header.Set("X-Amz-Security-Token", token)
	}

	signer := aws.NewV4Signer(c.Auth, "dynamodb", c.Region)
	signer.Sign(hreq)

	for attempt := attempts.Start(); attempt.Next(); {
		resp, err := c.HTTPClient.Do(hreq)

		if err != nil {
			if shouldRetry(err) {
				continue
			}
			return &Response{err, nil}
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return &Response{ErrFailedtoReadResponse, nil}
		}
		if resp.StatusCode != 200 {
			err = NewError(resp, body)
			if shouldRetry(err) {
				continue
			}
			return &Response{err, nil}
		}
		return &Response{nil, body}
	}
	return &Response{err, nil}
}

type Response struct {
	Error error

	json []byte
}

func (r *Response) Scan(result interface{}) error {
	if r.Error != nil {
		return r.Error
	}
	jerr := json.Unmarshal(r.json, result)
	if jerr != nil {
		return &UnexpectedResponseError{r.json, jerr}
	}
	return nil
}
