package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type (
	AttributeData               string
	ComparisonOperator          string
	ConditionalOperator         string
	IndexStatus                 string
	KeyType                     string
	ProjectionType              string
	ReturnConsumedCapacity      string
	ReturnItemCollectionMetrics string
	ReturnValues                string
	Select                      string
	TableStatus                 string
	UpdateAction                string
)

type (
	ScanFilter    map[string]Condition
	QueryFilter   map[string]Condition
	KeyConditions map[string]Condition
)

type ExpectedAttributeValue map[string]DeprecatedCondition

const (
	KeyTypeHash  KeyType = "HASH"
	KeyTypeRange KeyType = "RANGE"
)

const (
	TypeString AttributeType = "S"

	TypeNumber AttributeType = "N"
	TypeBinary AttributeType = "B"

	TypeStringSet AttributeType = "SS"
	TypeNumberSet AttributeType = "NS"
	TypeBinarySet AttributeType = "BS"
)

const (
	CmpOpEQ ComparisonOperator = "EQ"

	CmpOpLE ComparisonOperator = "LE"
	CmpOpLT ComparisonOperator = "LT"
	CmpOpGE ComparisonOperator = "GE"
	CmpOpGT ComparisonOperator = "GT"

	CmpOpNotNull ComparisonOperator = "NOT_NULL"
	CmpOpNull    ComparisonOperator = "NULL"

	CmpOpContains    ComparisonOperator = "CONTAINS"
	CmpOpNotContains ComparisonOperator = "NOT_CONTAINS"

	CmpOpBeginsWith ComparisonOperator = "BEGINS_WITH"

	CmpOpIn      ComparisonOperator = "IN"
	CmpOpBetween ComparisonOperator = "BETWEEN"
)

const (
	CondOpAnd ConditionalOperator = "AND"
	CondOpOr  ConditionalOperator = "OR"
)

const (
	ConsumedCapIndexes ReturnConsumedCapacity = "INDEXES"
	ConsumedCapTotal   ReturnConsumedCapacity = "TOTAL"
	ConsumedCapNone    ReturnConsumedCapacity = "NONE"
)

const (
	ProjectionTypeKeysOnly ProjectionType = "KEYS_ONLY"
	ProjectionTypeInclude  ProjectionType = "INCLUDE"
	ProjectionTypeAll      ProjectionType = "ALL"
)

const (
	ReturnItemCollectionMetricsSize ReturnItemCollectionMetrics = "SIZE"
	ReturnItemCollectionMetricsNone ReturnItemCollectionMetrics = "NONE"
)

const (
	ReturnValuesNone       ReturnValues = "NONE"
	ReturnValuesAllOld     ReturnValues = "ALL_OLD"
	ReturnValuesUpdatedOld ReturnValues = "UPDATED_OLD"
	ReturnValuesAllNew     ReturnValues = "ALL_NEW"
	ReturnValuesUpdatedNew ReturnValues = "UPDATED_NEW"
)

const (
	SelectAll          Select = "ALL_ATTRIBUTES"
	SelectAllProjected Select = "ALL_PROJECTED_ATTRIBUTES"
	SelectSpecific     Select = "SPECIFIC_ATTRIBUTES"
	SelectCount        Select = "COUNT"
)

const (
	ActionPut    UpdateAction = "PUT"
	ActionDelete UpdateAction = "DELETE"
	ActionAdd    UpdateAction = "ADD"
)

const (
	IndexStatusCreating IndexStatus = "CREATING"
	IndexStatusUpdating IndexStatus = "UPDATING"
	IndexStatusDeleting IndexStatus = "DELETING"
	IndexStatusActive   IndexStatus = "ACTIVE"
)

const (
	TableStatusCreating TableStatus = "CREATING"

	TableStatusUpdating TableStatus = "UPDATING"
	TableStatusDeleting TableStatus = "DELETING"
	TableStatusActive   TableStatus = "ACTIVE"
)

type AttributeType string

func (at AttributeType) IsSet() bool {
	switch at {
	case TypeStringSet, TypeNumberSet, TypeBinarySet:
		return true
	default:
		return false
	}
}

type AttributeDefinition struct {
	Name string        `json:"AttributeName"`
	Type AttributeType `json:"AttributeType"`
}

type AttributeValue struct {
	Type AttributeType
	Data []AttributeData
}

func (v AttributeValue) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case TypeString:
		return json.Marshal(stringAttributeValue{v.Data[0]})
	case TypeNumber:
		return json.Marshal(numberAttributeValue{v.Data[0]})
	case TypeBinary:
		return json.Marshal(binaryAttributeValue{v.Data[0]})
	case TypeStringSet:
		return json.Marshal(stringSetAttributeValue{v.Data})
	case TypeNumberSet:
		return json.Marshal(numberSetAttributeValue{v.Data})
	case TypeBinarySet:
		// TODO: encoding with base64
		return json.Marshal(binarySetAttributeValue{v.Data})
	}
	return nil, fmt.Errorf("dynamodb: failed to marshal '%v'", v)
}

func (v *AttributeValue) UnmarshalJSON(data []byte) error {
	// {"SS":"ABC"}
	// {"SS":["ABC"]}
	j := map[AttributeType]interface{}{}
	jerr := json.Unmarshal(data, &j)
	if jerr != nil {
		return nil
	}
	if len(j) != 1 {
		return errors.New("dynamodb: failed to decode to AttributeValue")
	}
	for at := range j {
		v.Type = at
		// TODO: decoding with base64
		if v.Type.IsSet() {
			// j[at] = ["ABC"]
			set := j[at].([]interface{})
			for i := range set {
				v.Data = append(v.Data, AttributeData(set[i].(string)))
			}
		} else {
			// j[at] = "ABC"
			v.Data = append(v.Data, AttributeData(j[at].(string)))
		}
	}
	return nil
}

type AttributeUpdate struct {
	Action UpdateAction
	Value  AttributeValue `json:",omitempty"`
}

func (au AttributeUpdate) MarshalJSON() ([]byte, error) {
	if au.Action == ActionDelete && !au.Value.Type.IsSet() {
		return json.Marshal(
			struct {
				Action UpdateAction
			}{
				Action: au.Action,
			},
		)
	}
	return json.Marshal(
		struct {
			Action UpdateAction
			Value  AttributeValue
		}{
			Action: au.Action,
			Value:  au.Value,
		},
	)
}

type BatchGetItemResult struct {
	ConsumedCapacity ConsumedCapacity `json:",omitempty"`
	Responses        map[string][]map[string]AttributeValue
	UnprocessedKeys  map[string]KeysAndAttributes
}

type BatchWriteItemResult struct {
	ConsumedCapacity      ConsumedCapacity `json:",omitempty"`
	ItemCollectionMetrics map[string][]ItemCollectionMetrics
	UnprocessedItems      map[string][]WriteRequest
}

type Capacity struct {
	CapacityUnits float64 `json:",omitempty"`
}

type Condition struct {
	AttributeValueList []AttributeValue `json:",omitempty"`
	ComparisonOperator ComparisonOperator
}

type ConsumedCapacity struct {
	CapacityUnits          float64             `json:",omitempty"`
	GlobalSecondaryIndexes map[string]Capacity `json:",omitempty"`
	LocalSecondaryIndexes  map[string]Capacity `json:",omitempty"`
	Table                  Capacity            `json:",omitempty"`
	TableName              string
}

// TODO: Dynalite is missing new Condition parameter as of writing
type DeprecatedCondition struct {
	Value  AttributeValue `json:",omitempty"`
	Exists bool           `json:",omitempty"`
}

func (c DeprecatedCondition) MarshalJSON() ([]byte, error) {
	if !c.Exists {
		return json.Marshal(
			struct {
				Exists bool
			}{
				Exists: c.Exists,
			},
		)
	}
	return json.Marshal(
		struct {
			Value  AttributeValue
			Exists bool
		}{
			Value:  c.Value,
			Exists: c.Exists,
		},
	)
}

type UpdateItemResult struct {
	Attributes            map[string]AttributeValue `json:",omitempty"`
	ConsumedCapacity      ConsumedCapacity          `json:",omitempty"`
	ItemCollectionMetrics ItemCollectionMetrics     `json:",omitempty"`
}

type UpdateTableResult struct {
	TableDescription TableDescription `json:",omitempty"`
}

type DeleteTableResult struct {
	TableDescription TableDescription `json:",omitempty"`
}

type DeleteItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      ConsumedCapacity
	ItemCollectionMetrics ItemCollectionMetrics
}

type DescribeTableResult struct {
	Table TableDescription `json:",omitempty"`
}

type GetItemResult struct {
	ConsumedCapacity ConsumedCapacity
	Item             map[string]AttributeValue
}

type GlobalSecondaryIndex struct {
	IndexName             string
	KeySchema             []KeySchemaElement
	Projection            Projection
	ProvisionedThroughput ProvisionedThroughput
}

type GlobalSecondaryIndexDescription struct {
	IndexName             string             `json:",omitempty"`
	IndexSizeBytes        int64              `json:",omitempty"`
	IndexStatus           IndexStatus        `json:",omitempty"`
	ItemCount             int64              `json:",omitempty"`
	KeySchema             []KeySchemaElement `json:",omitempty"`
	Projection            Projection         `json:",omitempty"`
	ProvisionedThroughput ProvisionedThroughputDescription
}

type GlobalSecondaryIndexAction struct {
	IndexName             string
	ProvisionedThroughput ProvisionedThroughput
}

type GlobalSecondaryIndexUpdate struct {
	Update GlobalSecondaryIndexAction `json:",omitempty"`
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   map[string]AttributeValue
	SizeEstimateRangeGB float64
}

type KeySchemaElement struct {
	AttributeName string
	KeyType       KeyType
}

type KeysAndAttributes struct {
	AttributesToGet []string `json:",omitempty"`
	ConsistentRead  bool     `json:",omitempty"`
	Keys            []map[string]AttributeValue
}

type ListTablesResult struct {
	LastEvaluatedTableName string   `json:",omitempty"`
	TableNames             []string `json:",omitempty"`
}

type LocalSecondaryIndex struct {
	IndexName  string
	KeySchema  []KeySchemaElement
	Projection Projection
}

type LocalSecondaryIndexDescription struct {
	IndexName      string             `json:",omitempty"`
	IndexSizeBytes int64              `json:",omitempty"`
	ItemCount      int64              `json:",omitempty"`
	KeySchema      []KeySchemaElement `json:",omitempty"`
	Projection     Projection         `json:",omitempty"`
}

type Projection struct {
	NonKeyAttributes []string       `json:",omitempty"`
	ProjectionType   ProjectionType `json:",omitempty"`
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

type ProvisionedThroughputDescription struct {
	LastDecreaseDateTime  float64 `json:",omitempty"`
	LastIncreaseDateTime  float64 `json:",omitempty"`
	NumberODecreasesToday int64   `json:",omitempty"`
	ReadCapacityUnits     int64   `json:",omitempty"`
	WriteCapacityUnits    int64   `json:",omitempty"`
}

type PutItemResult struct {
	Attributes            map[string]AttributeValue `json:",omitempty"`
	ConsumedCapacity      ConsumedCapacity          `json:",omitempty"`
	ItemCollectionMetrics ItemCollectionMetrics     `json:",omitempty"`
}

type QueryResult struct {
	ConsumedCapacity ConsumedCapacity            `json:",omitempty"`
	Count            int64                       `json:",omitempty"`
	Items            []map[string]AttributeValue `json:",omitempty"`
	LastEvaluatedKey map[string]AttributeValue   `json:",omitempty"`
	ScannedCount     int64
}

type ScanResult struct {
	ConsumedCapacity ConsumedCapacity            `json:",omitempty"`
	Count            int64                       `json:",omitempty"`
	Items            []map[string]AttributeValue `json:",omitempty"`
	LastEvaluatedKey map[string]AttributeValue   `json:",omitempty"`
	ScannedCount     int64
}

type TableDescription struct {
	AttributeDefinitions []AttributeDefinition `json:",omitempty"`
	// CreationDateTime looks like '1405152783.735'
	CreationDateTime       float64                           `json:",omitempty"`
	GlobalSecondaryIndexes []GlobalSecondaryIndexDescription `json:",omitempty"`
	ItemCount              int64                             `json:",omitempty"`
	KeySchema              []KeySchemaElement                `json:",omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndexDescription  `json:",omitempty"`
	ProvisionedThroughput  ProvisionedThroughputDescription  `json:",omitempty"`
	TableName              string                            `json:",omitempty"`
	TableSizeBytes         int64                             `json:",omitempty"`
	TableStatus            TableStatus                       `json:",omitempty"`
}

type CreateTableResult struct {
	TableDescription TableDescription `json:",omitempty"`
}

func NewString(val string) AttributeValue {
	return AttributeValue{
		Type: TypeString,
		Data: []AttributeData{
			AttributeData(val),
		},
	}
}

func NewStringSet(val ...string) AttributeValue {
	ad := make([]AttributeData, len(val))
	for i := range val {
		ad[i] = AttributeData(val[i])
	}
	return AttributeValue{
		Type: TypeStringSet,
		Data: ad,
	}
}

func NewNumber(val int) AttributeValue {
	return AttributeValue{
		Type: TypeNumber,
		Data: []AttributeData{
			AttributeData(strconv.Itoa(val)),
		},
	}
}

func NewNumberSet(val ...int) AttributeValue {
	ad := make([]AttributeData, len(val))
	for i := range val {
		ad[i] = AttributeData(strconv.Itoa(val[i]))
	}
	return AttributeValue{
		Type: TypeNumberSet,
		Data: ad,
	}
}

// Just for JSON-transport
type stringAttributeValue struct {
	S AttributeData
}

type numberAttributeValue struct {
	N AttributeData
}

type binaryAttributeValue struct {
	B AttributeData
}

type stringSetAttributeValue struct {
	SS []AttributeData
}

type numberSetAttributeValue struct {
	NS []AttributeData
}

type binarySetAttributeValue struct {
	BS []AttributeData
}
