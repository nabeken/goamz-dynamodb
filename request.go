package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Request struct {
	Target string
	Param  interface{}
}

type CreateTableRequest struct {
	AttributeDefinitions   []AttributeDefinition
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:",omitempty"`
	KeySchema              []KeySchemaElement
	LocalSecondaryIndexes  []LocalSecondaryIndex `json:",omitempty"`
	ProvisionedThroughput  ProvisionedThroughput
	TableName              string
}

type DeleteTableRequest struct {
	TableName string
}

type DescribeTableRequest struct {
	TableName string
}

type ListTablesRequest struct {
	ExclusiveStartTableName string `json:",omitempty"`
	Limit                   uint   `json:",omitempty"`
}

type UpdateTableRequest struct {
	GlobalSecondaryIndexUpdates []GlobalSecondaryIndexUpdate `json:",omitempty"`
	ProvisionedThroughput       ProvisionedThroughput        `json:",omitempty"`
	TableName                   string
}

type BatchGetItemRequest struct {
	RequestItems           map[string]KeysAndAttributes
	ReturnConsumedCapacity ReturnConsumedCapacity `json:",omitempty"`
}

type BatchWriteItemRequest struct {
	RequestItems                map[string][]WriteRequest
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
}

type DeleteItemRequest struct {
	ConditionalOperator         ConditionalOperator    `json:",omitempty"`
	Expected                    ExpectedAttributeValue `json:",omitempty"`
	Key                         map[string]AttributeValue
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
	TableName                   string
}

type DeleteRequest struct {
	Key map[string]AttributeValue `json:",omitempty"`
}

func (r DeleteRequest) IsEmpty() bool {
	return len(r.Key) == 0
}

type GetItemRequest struct {
	AttributesToGet        []string `json:",omitempty"`
	ConsistentRead         bool     `json:",omitempty"`
	Key                    map[string]AttributeValue
	ReturnConsumedCapacity ReturnConsumedCapacity `json:",omitempty"`
	TableName              string
}

type PutItemRequest struct {
	ConditionalOperator ConditionalOperator `json:",omitempty"`
	//Expected                    map[string]Condition `json:",omitempty"`
	Expected                    ExpectedAttributeValue `json:",omitempty"`
	Item                        map[string]AttributeValue
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
	TableName                   string
}

type PutRequest struct {
	Item map[string]AttributeValue `json:",omitempty"`
}

func (r PutRequest) IsEmpty() bool {
	return len(r.Item) == 0
}

type QueryRequest struct {
	AttributesToGet        []string                  `json:",omitempty"`
	ConditionalOperator    ConditionalOperator       `json:",omitempty"`
	ConsistentRead         bool                      `json:",omitempty"`
	ExclusiveStartKey      map[string]AttributeValue `json:",omitempty"`
	IndexName              string                    `json:",omitempty"`
	KeyConditions          KeyConditions
	Limit                  uint                   `json:",omitempty"`
	QueryFilter            QueryFilter            `json:",omitempty"`
	ReturnConsumedCapacity ReturnConsumedCapacity `json:",omitempty"`
	ScanIndexForward       bool                   `json:",omitempty"`
	Select                 Select                 `json:",omitempty"`
	TableName              string
}

type ScanRequest struct {
	AttributesToGet        []string                  `json:",omitempty"`
	ConditionalOperator    ConditionalOperator       `json:",omitempty"`
	ExclusiveStartKey      map[string]AttributeValue `json:",omitempty"`
	Limit                  uint                      `json:",omitempty"`
	ReturnConsumedCapacity ReturnConsumedCapacity    `json:",omitempty"`
	ScanFilter             ScanFilter                `json:",omitempty"`
	Segment                uint                      `json:",omitempty"`
	Select                 Select                    `json:",omitempty"`
	TableName              string
	TotalSegments          uint `json:",omitempty"`
}

type UpdateItemRequest struct {
	AttributeUpdates            map[string]AttributeUpdate `json:",omitempty"`
	ConditionalOperator         ConditionalOperator        `json:",omitempty"`
	Expected                    ExpectedAttributeValue     `json:",omitempty"`
	Key                         map[string]AttributeValue
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
	TableName                   string
}

type WriteRequest struct {
	DeleteRequest DeleteRequest `json:",omitempty"`
	PutRequest    PutRequest    `json:",omitempty"`
}

func (wr WriteRequest) MarshalJSON() ([]byte, error) {
	if wr.DeleteRequest.IsEmpty() && wr.PutRequest.IsEmpty() {
		return nil, errors.New("dynamodb: WriteRequest must be Put or Delete, not both")
	}
	switch {
	case wr.DeleteRequest.IsEmpty():
		return json.Marshal(
			struct {
				PutRequest PutRequest
			}{
				PutRequest: wr.PutRequest,
			},
		)
	case wr.PutRequest.IsEmpty():
		return json.Marshal(
			struct {
				DeleteRequest DeleteRequest
			}{
				DeleteRequest: wr.DeleteRequest,
			},
		)
	default:
		return nil, errors.New("dynamodb: WriteRequest must be Put or Delete")
	}
}

func Dump(r interface{}) {
	j, _ := json.Marshal(r)
	fmt.Println(string(j))
}
