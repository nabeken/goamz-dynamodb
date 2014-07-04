package dynamodb_test

import (
	"strconv"
	"testing"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BatchSuite struct {
	suite.Suite
	DynamoDBTest

	numOfRecords int
}

func (s *BatchSuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTestBatch",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
			dynamodb.AttributeDefinition{"TestRangeKey", "N"},
		},
		KeySchema: []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
			dynamodb.KeySchema{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB(s.T())
}

func (s *BatchSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		attrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("Attr", ai),
		}
		if ok, err := s.table.PutItem("HashKeyVal"+ai, ai, attrs); !ok {
			s.T().Fatal(err)
		}
	}
}

func (s *BatchSuite) TestBatchGet() {
	s.createDummy()
	b := dynamodb.BatchGetItem{
		s.server,
		map[*dynamodb.Table][]dynamodb.Key{
			s.table: []dynamodb.Key{
				dynamodb.Key{"HashKeyVal0", "0"},
				dynamodb.Key{"HashKeyVal1", "1"},
				dynamodb.Key{"HashKeyVal2", "2"},
			},
		},
	}
	ret, err := b.Execute()
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), len(ret["DynamoDBTestBatch"]), 3)
		for i := range ret["DynamoDBTest"] {
			ia, err := strconv.Atoi(ret["DynamoDBTestBatch"][i]["TestRangeKey"].Value)
			assert.NoError(s.T(), err)
			assert.True(s.T(), ia <= 2)
		}
	}
}

func (s *BatchSuite) TestBatchWrite() {
	s.createDummy()

	attrs := [][]dynamodb.Attribute{}
	for i := 0; i < 10; i++ {
		ai := strconv.Itoa(i)
		attrs = append(attrs, []dynamodb.Attribute{
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_STRING,
				Name:  "TestHashKey",
				Value: "HashKeyVal" + ai,
			},
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_NUMBER,
				Name:  "TestRangeKey",
				Value: ai,
			},
		})
	}

	b := dynamodb.BatchWriteItem{
		s.server,
		map[*dynamodb.Table]map[string][][]dynamodb.Attribute{
			s.table: map[string][][]dynamodb.Attribute{
				"Delete": attrs,
			},
		},
	}
	ret, err := b.Execute()
	if assert.NoError(s.T(), err) {
		// No unprocessed item
		assert.Equal(s.T(), 0, len(ret))
	}

	scanRet, err := s.table.Scan(nil)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 90, len(scanRet))
	}
}

func TestBatch(t *testing.T) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	suite.Run(t, new(BatchSuite))
}
