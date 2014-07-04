package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

const (
	TypeString AttributeType = "S"
	TypeNumber AttributeType = "N"
	TypeBinary AttributeType = "B"

	TypeStringSet AttributeType = "SS"
	TypeNumberSet AttributeType = "NS"
	TypeBinarySet AttributeType = "BS"
)

type ComparisonOperator string

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

	CmpOpIn ComparisonOperator = "IN"

	CmpOpBetween ComparisonOperator = "BETWEEN"
)

type ConditionalOperator string

const (
	CondOpAnd ConditionalOperator = "AND"
	CondOpOr  ConditionalOperator = "OR"
)

type ConsumedCapacity string

const (
	ConsumedCapIndexes ConsumedCapacity = "INDEXES"
	ConsumedCapTotal   ConsumedCapacity = "TOTAL"
	ConsumedCapNone    ConsumedCapacity = "NONE"
)

type ItemCollectionMetrics string

const (
	ItemCollectionMetricsSize ItemCollectionMetrics = "SIZE"
	ItemCollectionMetricsNone ItemCollectionMetrics = "NONE"
)

type ReturnValues string

const (
	ReturnValuesNone       ReturnValues = "NONE"
	ReturnValuesAllOld     ReturnValues = "ALL_OLD"
	ReturnValuesUpdatedOld ReturnValues = "UPDATED_OLD"
	ReturnValuesAllNew     ReturnValues = "ALL_NEW"
	ReturnValuesUpdatedNew ReturnValues = "UPDATED_NEW"
)

type Select string

const (
	SelectAll          Select = "ALL_ATTRIBUTES"
	SelectAllProjected Select = "ALL_PROJECTED_ATTRIBUTES"
	SelectSpecific     Select = "SPECIFIC_ATTRIBUTES"
	SelectCount        Select = "COUNT"
)

type UpdateAction string

const (
	ActionPut    UpdateAction = "PUT"
	ActionDelete UpdateAction = "DELETE"
	ActionAdd    UpdateAction = "ADD"
)

func (at AttributeType) IsSet() bool {
	switch at {
	case TypeStringSet, TypeNumberSet, TypeBinarySet:
		return true
	default:
		return false
	}
}

const (
	TYPE_STRING = "S"
	TYPE_NUMBER = "N"
	TYPE_BINARY = "B"

	TYPE_STRING_SET = "SS"
	TYPE_NUMBER_SET = "NS"
	TYPE_BINARY_SET = "BS"
)

const (
	COMPARISON_EQUAL                    = "EQ"
	COMPARISON_NOT_EQUAL                = "NE"
	COMPARISON_LESS_THAN_OR_EQUAL       = "LE"
	COMPARISON_LESS_THAN                = "LT"
	COMPARISON_GREATER_THAN_OR_EQUAL    = "GE"
	COMPARISON_GREATER_THAN             = "GT"
	COMPARISON_ATTRIBUTE_EXISTS         = "NOT_NULL"
	COMPARISON_ATTRIBUTE_DOES_NOT_EXIST = "NULL"
	COMPARISON_CONTAINS                 = "CONTAINS"
	COMPARISON_DOES_NOT_CONTAIN         = "NOT_CONTAINS"
	COMPARISON_BEGINS_WITH              = "BEGINS_WITH"
	COMPARISON_IN                       = "IN"
	COMPARISON_BETWEEN                  = "BETWEEN"
)

type Key struct {
	HashKey  string
	RangeKey string
}

type PrimaryKey struct {
	KeyAttribute   *Attribute
	RangeAttribute *Attribute
}

type (
	AttributeData string
	AttributeType string
)

type Attribute struct {
	Type      string
	Name      string
	Value     string
	SetValues []string
	Exists    bool
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

type KeysAndAttributes struct {
	AttributesToGet []string
	ConsistentRead  bool
	Keys            []AttributeValue
}

type Condition struct {
	AttributeValueList []AttributeValue `json:",omitempty"`
	ComparisonOperator ComparisonOperator
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

type AttributeComparison struct {
	AttributeName      string
	ComparisonOperator string
	AttributeValueList []Attribute // contains attributes with only types and value (name is ignored)
}

func NewEqualInt64AttributeComparison(attributeName string, equalToValue int64) *AttributeComparison {
	numeric := NewNumericAttribute(attributeName, strconv.FormatInt(equalToValue, 10))
	return &AttributeComparison{attributeName,
		COMPARISON_EQUAL,
		[]Attribute{*numeric},
	}
}

func NewEqualStringAttributeComparison(attributeName string, equalToValue string) *AttributeComparison {
	str := NewStringAttribute(attributeName, equalToValue)
	return &AttributeComparison{attributeName,
		COMPARISON_EQUAL,
		[]Attribute{*str},
	}
}

func NewStringAttributeComparison(attributeName string, comparisonOperator string, value string) *AttributeComparison {
	valueToCompare := NewStringAttribute(attributeName, value)
	return &AttributeComparison{attributeName,
		comparisonOperator,
		[]Attribute{*valueToCompare},
	}
}

func NewNumericAttributeComparison(attributeName string, comparisonOperator string, value int64) *AttributeComparison {
	valueToCompare := NewNumericAttribute(attributeName, strconv.FormatInt(value, 10))
	return &AttributeComparison{attributeName,
		comparisonOperator,
		[]Attribute{*valueToCompare},
	}
}

func NewBinaryAttributeComparison(attributeName string, comparisonOperator string, value bool) *AttributeComparison {
	valueToCompare := NewBinaryAttribute(attributeName, strconv.FormatBool(value))
	return &AttributeComparison{attributeName,
		comparisonOperator,
		[]Attribute{*valueToCompare},
	}
}

func NewStringAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  TYPE_STRING,
		Name:  name,
		Value: value,
	}
}

func NewNumericAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  TYPE_NUMBER,
		Name:  name,
		Value: value,
	}
}

func NewBinaryAttribute(name string, value string) *Attribute {
	return &Attribute{
		Type:  TYPE_BINARY,
		Name:  name,
		Value: value,
	}
}

func NewStringSetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      TYPE_STRING_SET,
		Name:      name,
		SetValues: values,
	}
}

func NewNumericSetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      TYPE_NUMBER_SET,
		Name:      name,
		SetValues: values,
	}
}

func NewBinarySetAttribute(name string, values []string) *Attribute {
	return &Attribute{
		Type:      TYPE_BINARY_SET,
		Name:      name,
		SetValues: values,
	}
}

func (a *Attribute) SetType() bool {
	switch a.Type {
	case TYPE_BINARY_SET, TYPE_NUMBER_SET, TYPE_STRING_SET:
		return true
	}
	return false
}

func (a *Attribute) SetExists(exists bool) *Attribute {
	a.Exists = exists
	return a
}

func (k *PrimaryKey) HasRange() bool {
	return k.RangeAttribute != nil
}

// Useful when you may have many goroutines using a primary key, so they don't fuxor up your values.
func (k *PrimaryKey) Clone(h string, r string) []Attribute {
	pk := &Attribute{
		Type:  k.KeyAttribute.Type,
		Name:  k.KeyAttribute.Name,
		Value: h,
	}

	result := []Attribute{*pk}

	if k.HasRange() {
		rk := &Attribute{
			Type:  k.RangeAttribute.Type,
			Name:  k.RangeAttribute.Name,
			Value: r,
		}

		result = append(result, *rk)
	}

	return result
}
