package dynamodb_test

import (
	"encoding/json"
	"testing"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestAttributeValue_String(t *testing.T) {
	av := &dynamodb.AttributeValue{
		Type: dynamodb.TypeString,
		Data: []dynamodb.AttributeData{"STRING"},
	}
	j, jerr := json.Marshal(av)
	assert.NoError(t, jerr)
	assert.Equal(t, `{"S":"STRING"}`, string(j))

	nav := &dynamodb.AttributeValue{}
	jerr = json.Unmarshal(j, nav)
	assert.NoError(t, jerr)
	assert.Equal(t, av, nav)
}

func TestAttributeValue_StringSet(t *testing.T) {
	av := &dynamodb.AttributeValue{
		Type: dynamodb.TypeStringSet,
		Data: []dynamodb.AttributeData{"STRING1", "STRING2"},
	}
	j, jerr := json.Marshal(av)
	assert.NoError(t, jerr)
	assert.Equal(t, `{"SS":["STRING1","STRING2"]}`, string(j))

	nav := &dynamodb.AttributeValue{}
	jerr = json.Unmarshal(j, nav)
	assert.NoError(t, jerr)
	assert.Equal(t, av, nav)
}

func TestAttributeValue_Number(t *testing.T) {
	av := &dynamodb.AttributeValue{
		Type: dynamodb.TypeNumber,
		Data: []dynamodb.AttributeData{"123456789"},
	}
	j, jerr := json.Marshal(av)
	assert.NoError(t, jerr)
	assert.Equal(t, `{"N":"123456789"}`, string(j))

	nav := &dynamodb.AttributeValue{}
	jerr = json.Unmarshal(j, nav)
	assert.NoError(t, jerr)
	assert.Equal(t, av, nav)
}

func TestAttributeValue_NumberSet(t *testing.T) {
	av := &dynamodb.AttributeValue{
		Type: dynamodb.TypeNumberSet,
		Data: []dynamodb.AttributeData{"123456789", "23456789"},
	}
	j, jerr := json.Marshal(av)
	assert.NoError(t, jerr)
	assert.Equal(t, `{"NS":["123456789","23456789"]}`, string(j))

	nav := &dynamodb.AttributeValue{}
	jerr = json.Unmarshal(j, nav)
	assert.NoError(t, jerr)
	assert.Equal(t, av, nav)
}
