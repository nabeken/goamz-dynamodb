package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
)

type RawRequest struct {
	Target string
	Param  interface{}
}

type CreateTable Table

type ListTablesOption struct {
	ExclusiveStartTableName string `json:",omitempty"`
	Limit                   uint   `json:",omitempty"`
}

type UpdateTableOption struct {
	GlobalSecondaryIndexUpdates []GlobalSecondaryIndexUpdate `json:",omitempty"`
	ProvisionedThroughput       ProvisionedThroughput        `json:",omitempty"`
}

type BatchGetItemOption struct {
	ReturnConsumedCapacity ReturnConsumedCapacity `json:",omitempty"`
}

type BatchWriteItemOption struct {
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
}

type DeleteItemOption struct {
	ConditionalOperator         ConditionalOperator         `json:",omitempty"`
	Expected                    ExpectedAttributeValue      `json:",omitempty"`
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
}

type DeleteRequest struct {
	Key map[string]AttributeValue `json:",omitempty"`
}

func (r DeleteRequest) IsEmpty() bool {
	return len(r.Key) == 0
}

type GetItemOption struct {
	AttributesToGet        []string               `json:",omitempty"`
	ConsistentRead         bool                   `json:",omitempty"`
	ReturnConsumedCapacity ReturnConsumedCapacity `json:",omitempty"`
}

type PutItemOption struct {
	ConditionalOperator ConditionalOperator `json:",omitempty"`
	//Expected                    map[string]Condition `json:",omitempty"`
	Expected                    ExpectedAttributeValue      `json:",omitempty"`
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
}

type PutRequest struct {
	Item Item `json:",omitempty"`
}

func (r PutRequest) IsEmpty() bool {
	return len(r.Item) == 0
}

type QueryOption struct {
	AttributesToGet        []string                  `json:",omitempty"`
	ConditionalOperator    ConditionalOperator       `json:",omitempty"`
	ConsistentRead         bool                      `json:",omitempty"`
	ExclusiveStartKey      map[string]AttributeValue `json:",omitempty"`
	IndexName              string                    `json:",omitempty"`
	Limit                  uint                      `json:",omitempty"`
	QueryFilter            QueryFilter               `json:",omitempty"`
	ReturnConsumedCapacity ReturnConsumedCapacity    `json:",omitempty"`
	ScanIndexForward       bool                      `json:",omitempty"`
	Select                 Select                    `json:",omitempty"`
}

type ScanOption struct {
	AttributesToGet        []string                  `json:",omitempty"`
	ConditionalOperator    ConditionalOperator       `json:",omitempty"`
	ExclusiveStartKey      map[string]AttributeValue `json:",omitempty"`
	Limit                  uint                      `json:",omitempty"`
	ReturnConsumedCapacity ReturnConsumedCapacity    `json:",omitempty"`
	ScanFilter             ScanFilter                `json:",omitempty"`
	Segment                uint                      `json:",omitempty"`
	Select                 Select                    `json:",omitempty"`
	TotalSegments          uint                      `json:",omitempty"`
}

type UpdateItemOption struct {
	AttributeUpdates            map[string]AttributeUpdate  `json:",omitempty"`
	ConditionalOperator         ConditionalOperator         `json:",omitempty"`
	Expected                    ExpectedAttributeValue      `json:",omitempty"`
	ReturnConsumedCapacity      ReturnConsumedCapacity      `json:",omitempty"`
	ReturnItemCollectionMetrics ReturnItemCollectionMetrics `json:",omitempty"`
	ReturnValues                ReturnValues                `json:",omitempty"`
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
