package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TableSuite struct {
	suite.Suite
	DynamoDBTest
}

func (s *TableSuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTest",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
			dynamodb.AttributeDefinition{"TestRangeKey", "N"},
		},
		KeySchema: []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
			dynamodb.KeySchema{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
	}
	s.CreateNewTable = true
	s.SetupDB(s.T())
}

func (s *TableSuite) TestListTable() {
	tables, err := s.server.ListTables()
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), len(tables), 1)
	assert.True(s.T(), findTableByName(tables, s.TableDescription.TableName))
}

type TableGSISuite struct {
	suite.Suite
	DynamoDBTest
}

func (s *TableGSISuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTestGSI",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"UserId", "S"},
			dynamodb.AttributeDefinition{"OSType", "S"},
			dynamodb.AttributeDefinition{"IMSI", "S"},
		},
		KeySchema: []dynamodb.KeySchema{
			dynamodb.KeySchema{"UserId", "HASH"},
			dynamodb.KeySchema{"OSType", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  5,
			WriteCapacityUnits: 5,
		},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndex{
			dynamodb.GlobalSecondaryIndex{
				IndexName: "IMSIIndex",
				KeySchema: []dynamodb.KeySchema{
					dynamodb.KeySchema{"IMSI", "HASH"},
				},
				Projection: dynamodb.Projection{
					ProjectionType: "KEYS_ONLY",
				},
				ProvisionedThroughput: dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  5,
					WriteCapacityUnits: 5,
				},
			},
		},
	}

	s.CreateNewTable = true
	s.SetupDB(s.T())
}

func (s *TableGSISuite) TestListTable() {
	tables, err := s.server.ListTables()
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), len(tables), 1)
	assert.True(s.T(), findTableByName(tables, s.TableDescription.TableName))
}

func (s *TableGSISuite) TestDescribeTable() {
	td, err := s.server.DescribeTable(s.TableDescription.TableName)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), len(td.GlobalSecondaryIndexes), 1)
	}
}

func (s *TableGSISuite) TestUpdateTable() {
	_, err := s.server.UpdateTable(dynamodb.UpdateTableQuery{
		GlobalSecondaryIndexUpdates: []dynamodb.GlobalSecondaryIndexUpdate{
			dynamodb.GlobalSecondaryIndexUpdate{
				Update: dynamodb.GlobalSecondaryIndexAction{
					IndexName: "IMSIIndex",
					ProvisionedThroughput: dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  10,
						WriteCapacityUnits: 10,
					},
				},
			},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
		TableName: s.TableDescription.TableName,
	})
	if assert.NoError(s.T(), err) {
		timeoutChan := time.After(timeout)
		done := handleAction(func(done chan struct{}) {
			td, terr := s.server.DescribeTable(s.TableDescription.TableName)
			if assert.NoError(s.T(), terr) {
				if td.ProvisionedThroughput.ReadCapacityUnits == 10 &&
					td.GlobalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits == 10 {
					close(done)
					return
				}
			}
			s.T().Log("Waiting for ProvisionedThroughput updated...")
			time.Sleep(3 * time.Second)
		})

		select {
		case <-done:
			td, terr := s.server.DescribeTable(s.TableDescription.TableName)
			if assert.NoError(s.T(), terr) {
				assert.Equal(s.T(), 10, td.ProvisionedThroughput.ReadCapacityUnits)
				assert.Equal(s.T(), 10, td.ProvisionedThroughput.WriteCapacityUnits)
				assert.Equal(s.T(), 10, td.GlobalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
				assert.Equal(s.T(), 10, td.GlobalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
			}
		case <-timeoutChan:
			close(done)
			s.T().Errorf("Expect ProvisionedThroughput to be changed, but timed out")
		}
	}
}

type ItemSuite struct {
	suite.Suite
	DynamoDBTest

	WithRange bool
	TableName string
}

func (s *ItemSuite) SetupSuite() {
	var ks []dynamodb.KeySchema
	var ad []dynamodb.AttributeDefinition
	if s.WithRange {
		ks = []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
			dynamodb.KeySchema{"TestRangeKey", "RANGE"},
		}
		ad = []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
			dynamodb.AttributeDefinition{"TestRangeKey", "N"},
		}
	} else {
		ks = []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
		}
		ad = []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
		}
	}
	s.TableDescription = dynamodb.TableDescription{
		TableName:            "DynamoDBTestItem",
		AttributeDefinitions: ad,
		KeySchema:            ks,
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	if s.TableName != "" {
		s.TableDescription.TableName = s.TableName
	}
	s.CreateNewTable = true
	s.SetupDB(s.T())
}

func (s *ItemSuite) TestConditionalAddAttributesItem() {
	if s.WithRange {
		s.T().Skip("No rangekey test is not required")
	}

	attrs := []dynamodb.Attribute{
		*dynamodb.NewNumericAttribute("AttrN", "10"),
	}
	pk := &dynamodb.Key{HashKey: "NewHashKeyVal"}

	// Put
	if ok, err := s.table.PutItem("NewHashKeyVal", "", attrs); !ok {
		s.T().Fatal(err)
	}

	{
		// Put with condition failed
		expected := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("AttrN", "0").SetExists(true),
			*dynamodb.NewStringAttribute("AttrNotExists", "").SetExists(false),
		}
		// Add attributes with condition failed
		if ok, err := s.table.ConditionalAddAttributes(pk, attrs, expected); ok {
			s.T().Errorf("Expect condition does not meet.")
		} else {
			derr := err.(*dynamodb.Error)
			assert.Equal(s.T(), derr.Status, "400 Bad Request")
			assert.Equal(s.T(), derr.Code, "ConditionalCheckFailedException")
		}
	}
}

func (s *ItemSuite) TestConditionalPutUpdateDeleteItem() {
	if s.WithRange {
		// No rangekey test required
		s.T().Skip("No rangekey test is not required")
	}

	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("Attr1", "Attr1Val"),
	}
	pk := &dynamodb.Key{HashKey: "NewHashKeyVal"}

	// Put
	if ok, err := s.table.PutItem("NewHashKeyVal", "", attrs); !ok {
		s.T().Fatal(err)
	}

	{
		// Put with condition failed
		expected := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "expectedAttr1Val").SetExists(true),
			*dynamodb.NewStringAttribute("AttrNotExists", "").SetExists(false),
		}
		if ok, err := s.table.ConditionalPutItem("NewHashKeyVal", "", attrs, expected); ok {
			s.T().Errorf("Expect condition does not meet.")
		} else {
			derr := err.(*dynamodb.Error)
			assert.Equal(s.T(), derr.Status, "400 Bad Request")
			assert.Equal(s.T(), derr.Code, "ConditionalCheckFailedException")
		}

		// Update attributes with condition failed
		if ok, err := s.table.ConditionalUpdateAttributes(pk, attrs, expected); ok {
			s.T().Errorf("Expect condition does not meet.")
		} else {
			derr := err.(*dynamodb.Error)
			assert.Equal(s.T(), derr.Status, "400 Bad Request")
			assert.Equal(s.T(), derr.Code, "ConditionalCheckFailedException")
		}

		// Delete attributes with condition failed
		if ok, err := s.table.ConditionalDeleteAttributes(pk, attrs, expected); ok {
			s.T().Errorf("Expect condition does not meet.")
		} else {
			derr := err.(*dynamodb.Error)
			assert.Equal(s.T(), derr.Status, "400 Bad Request")
			assert.Equal(s.T(), derr.Code, "ConditionalCheckFailedException")
		}
	}

	{
		expected := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "Attr1Val").SetExists(true),
		}

		// Add attributes with condition met
		addNewAttrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("AddNewAttr1", "10"),
			*dynamodb.NewNumericAttribute("AddNewAttr2", "20"),
		}
		if ok, err := s.table.ConditionalAddAttributes(pk, addNewAttrs, nil); !ok {
			s.T().Errorf("Expect condition met. %s", err)
		}

		// Update attributes with condition met
		updateAttrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("AddNewAttr1", "100"),
		}
		if ok, err := s.table.ConditionalUpdateAttributes(pk, updateAttrs, expected); !ok {
			s.T().Errorf("Expect condition met. %s", err)
		}

		// Delete attributes with condition met
		deleteAttrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("AddNewAttr2", ""),
		}
		if ok, err := s.table.ConditionalDeleteAttributes(pk, deleteAttrs, expected); !ok {
			s.T().Errorf("Expect condition met. %s", err)
		}

		// Get to verify operations that condition are met
		item, err := s.table.GetItem(pk)
		if err != nil {
			s.T().Fatal(err)
		}

		if val, ok := item["AddNewAttr1"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewNumericAttribute("AddNewAttr1", "100"))
		} else {
			s.T().Error("Expect AddNewAttr1 attribute to be added and updated")
		}

		if _, ok := item["AddNewAttr2"]; ok {
			s.T().Error("Expect AddNewAttr2 attribute to be deleted")
		}
	}

	{
		// Put with condition met
		expected := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "Attr1Val").SetExists(true),
		}
		newattrs := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "Attr2Val"),
		}
		if ok, err := s.table.ConditionalPutItem("NewHashKeyVal", "", newattrs, expected); !ok {
			s.T().Errorf("Expect condition met. %s", err)
		}

		// Get to verify Put operation that condition are met
		item, err := s.table.GetItem(pk)
		if err != nil {
			s.T().Fatal(err)
		}

		if val, ok := item["Attr1"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewStringAttribute("Attr1", "Attr2Val"))
		} else {
			s.T().Error("Expect Attr1 attribute to be updated")
		}
	}

	{
		// Delete with condition failed
		expected := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "expectedAttr1Val").SetExists(true),
		}
		if ok, err := s.table.ConditionalDeleteItem(pk, expected); ok {
			s.T().Errorf("Expect condition does not meet.")
		} else {
			derr := err.(*dynamodb.Error)
			assert.Equal(s.T(), derr.Status, "400 Bad Request")
			assert.Equal(s.T(), derr.Code, "ConditionalCheckFailedException")
		}
	}

	{
		// Delete with condition met
		expected := []dynamodb.Attribute{
			*dynamodb.NewStringAttribute("Attr1", "Attr2Val").SetExists(true),
		}
		if ok, _ := s.table.ConditionalDeleteItem(pk, expected); !ok {
			s.T().Errorf("Expect condition met.")
		}

		// Get to verify Delete operation
		_, err := s.table.GetItem(pk)
		assert.Error(s.T(), err, "dynamodb: item not found")
	}
}

func (s *ItemSuite) TestPutGetDeleteItem() {
	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("Attr1", "Attr1Val"),
	}

	var rk string
	if s.WithRange {
		rk = "1"
	}

	// Put
	if ok, err := s.table.PutItem("NewHashKeyVal", rk, attrs); !ok {
		s.T().Fatal(err)
	}

	// Get to verify Put operation
	pk := &dynamodb.Key{HashKey: "NewHashKeyVal", RangeKey: rk}
	item, err := s.table.GetItem(pk)
	if err != nil {
		s.T().Fatal(err)
	}

	if val, ok := item["TestHashKey"]; ok {
		assert.Equal(s.T(), val, dynamodb.NewStringAttribute("TestHashKey", "NewHashKeyVal"))
	} else {
		s.T().Error("Expect TestHashKey to be found")
	}

	if s.WithRange {
		if val, ok := item["TestRangeKey"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewNumericAttribute("TestRangeKey", "1"))
		} else {
			s.T().Error("Expect TestRangeKey to be found")
		}
	}

	// Delete
	if ok, _ := s.table.DeleteItem(pk); !ok {
		s.T().Fatal(err)
	}

	// Get to verify Delete operation
	_, err = s.table.GetItem(pk)
	assert.Error(s.T(), err, "dynamodb: item not found")
}

func (s *ItemSuite) TestUpdateItem() {
	attrs := []dynamodb.Attribute{
		*dynamodb.NewNumericAttribute("count", "0"),
	}

	var rk string
	if s.WithRange {
		rk = "1"
	}

	if ok, err := s.table.PutItem("NewHashKeyVal", rk, attrs); !ok {
		s.T().Fatal(err)
	}

	// UpdateItem with Add
	attrs = []dynamodb.Attribute{
		*dynamodb.NewNumericAttribute("count", "10"),
	}
	pk := &dynamodb.Key{HashKey: "NewHashKeyVal", RangeKey: rk}
	if ok, err := s.table.AddAttributes(pk, attrs); !ok {
		s.T().Error(err)
	}

	// Get to verify Add operation
	if item, err := s.table.GetItemConsistent(pk, true); err != nil {
		s.T().Error(err)
	} else {
		if val, ok := item["count"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewNumericAttribute("count", "10"))
		} else {
			s.T().Error("Expect count to be found")
		}
	}

	// UpdateItem with Put
	attrs = []dynamodb.Attribute{
		*dynamodb.NewNumericAttribute("count", "100"),
	}
	if ok, err := s.table.UpdateAttributes(pk, attrs); !ok {
		s.T().Error(err)
	}

	// Get to verify Put operation
	if item, err := s.table.GetItem(pk); err != nil {
		s.T().Fatal(err)
	} else {
		if val, ok := item["count"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewNumericAttribute("count", "100"))
		} else {
			s.T().Error("Expect count to be found")
		}
	}

	// UpdateItem with Delete
	attrs = []dynamodb.Attribute{
		*dynamodb.NewNumericAttribute("count", ""),
	}
	if ok, err := s.table.DeleteAttributes(pk, attrs); !ok {
		s.T().Error(err)
	}

	// Get to verify Delete operation
	if item, err := s.table.GetItem(pk); err != nil {
		s.T().Error(err)
	} else {
		if _, ok := item["count"]; ok {
			s.T().Error("Expect count not to be found")
		}
	}
}

func (s *ItemSuite) TestUpdateItemWithSet() {
	attrs := []dynamodb.Attribute{
		*dynamodb.NewStringSetAttribute("list", []string{"A", "B"}),
	}

	var rk string
	if s.WithRange {
		rk = "1"
	}

	if ok, err := s.table.PutItem("NewHashKeyVal", rk, attrs); !ok {
		s.T().Error(err)
	}

	// UpdateItem with Add
	attrs = []dynamodb.Attribute{
		*dynamodb.NewStringSetAttribute("list", []string{"C"}),
	}
	pk := &dynamodb.Key{HashKey: "NewHashKeyVal", RangeKey: rk}
	if ok, err := s.table.AddAttributes(pk, attrs); !ok {
		s.T().Error(err)
	}

	// Get to verify Add operation
	if item, err := s.table.GetItem(pk); err != nil {
		s.T().Error(err)
	} else {
		if val, ok := item["list"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewStringSetAttribute("list", []string{"A", "B", "C"}))
		} else {
			s.T().Error("Expect count to be found")
		}
	}

	// UpdateItem with Delete
	attrs = []dynamodb.Attribute{
		*dynamodb.NewStringSetAttribute("list", []string{"A"}),
	}
	if ok, err := s.table.DeleteAttributes(pk, attrs); !ok {
		s.T().Error(err)
	}

	// Get to verify Delete operation
	if item, err := s.table.GetItem(pk); err != nil {
		s.T().Error(err)
	} else {
		if val, ok := item["list"]; ok {
			assert.Equal(s.T(), val, dynamodb.NewStringSetAttribute("list", []string{"B", "C"}))
		} else {
			s.T().Error("Expect list to be remained")
		}
	}
}

type QuerySuite struct {
	suite.Suite
	DynamoDBTest

	numOfRecords int
}

func (s *QuerySuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTestQuery",
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

func (s *QuerySuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		attrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("Attr", ai),
		}
		if ok, err := s.table.PutItem("HashKeyVal", ai, attrs); !ok {
			s.T().Fatal(err)
		}
	}
}

func (s *QuerySuite) TestQuery() {
	s.createDummy()
	atc := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName:      "TestHashKey",
			ComparisonOperator: "EQ",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_STRING,
					Value: "HashKeyVal",
				},
			},
		},
		dynamodb.AttributeComparison{
			AttributeName:      "TestRangeKey",
			ComparisonOperator: "LT",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_NUMBER,
					Value: "1",
				},
			},
		},
	}
	ret, err := s.table.Query(atc)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 1, len(ret))
		assert.Equal(s.T(), "0", ret[0]["TestRangeKey"].Value)
	}
}

func (s *QuerySuite) TestLimitedQuery() {
	s.createDummy()
	atc := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName:      "TestHashKey",
			ComparisonOperator: "EQ",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_STRING,
					Value: "HashKeyVal",
				},
			},
		},
	}
	ret, err := s.table.LimitedQuery(atc, 1)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 1, len(ret))
		assert.Equal(s.T(), "0", ret[0]["TestRangeKey"].Value)
	}
}

func (s *QuerySuite) TestCountQuery() {
	s.createDummy()
	atc := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName:      "TestHashKey",
			ComparisonOperator: "EQ",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_STRING,
					Value: "HashKeyVal",
				},
			},
		},
		dynamodb.AttributeComparison{
			AttributeName:      "TestRangeKey",
			ComparisonOperator: "LT",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_NUMBER,
					Value: "2",
				},
			},
		},
	}
	c, err := s.table.CountQuery(atc)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 2, c)
	}
}

type QueryOnIndexSuite struct {
	suite.Suite
	DynamoDBTest

	numOfRecords int
}

func (s *QueryOnIndexSuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTestQueryOnIndex",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
			dynamodb.AttributeDefinition{"TestRangeKey", "N"},
			dynamodb.AttributeDefinition{"LSIKey", "N"},
			dynamodb.AttributeDefinition{"GSIKey", "N"},
		},
		KeySchema: []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
			dynamodb.KeySchema{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndex{
			dynamodb.GlobalSecondaryIndex{
				IndexName: "GSI",
				KeySchema: []dynamodb.KeySchema{
					dynamodb.KeySchema{"GSIKey", "HASH"},
				},
				Projection: dynamodb.Projection{
					ProjectionType: "KEYS_ONLY",
				},
				ProvisionedThroughput: dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  10,
					WriteCapacityUnits: 10,
				},
			},
		},
		LocalSecondaryIndexes: []dynamodb.LocalSecondaryIndex{
			dynamodb.LocalSecondaryIndex{
				IndexName: "LSI",
				KeySchema: []dynamodb.KeySchema{
					dynamodb.KeySchema{"TestHashKey", "HASH"},
					dynamodb.KeySchema{"LSIKey", "RANGE"},
				},
				Projection: dynamodb.Projection{
					ProjectionType: "KEYS_ONLY",
				},
			},
		},
	}

	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB(s.T())
}

func (s *QueryOnIndexSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		attrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("GSIKey", ai),
			*dynamodb.NewNumericAttribute("LSIKey", ai),
		}
		if ok, err := s.table.PutItem("HashKeyVal", ai, attrs); !ok {
			s.T().Fatal(err)
		}
	}
}

func (s *QueryOnIndexSuite) TestQueryOnIndex() {
	s.createDummy()
	atc := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName:      "GSIKey",
			ComparisonOperator: "EQ",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_NUMBER,
					Value: "80",
				},
			},
		},
	}
	ret, err := s.table.QueryOnIndex(atc, "GSI")
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 1, len(ret))
		assert.Equal(s.T(), "80", ret[0]["TestRangeKey"].Value)
	}
}

func (s *QueryOnIndexSuite) TestLimitedQueryOnIndex() {
	s.createDummy()
	atc := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName:      "TestHashKey",
			ComparisonOperator: "EQ",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_STRING,
					Value: "HashKeyVal",
				},
			},
		},
		dynamodb.AttributeComparison{
			AttributeName:      "LSIKey",
			ComparisonOperator: "LT",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  dynamodb.TYPE_NUMBER,
					Value: "10",
				},
			},
		},
	}
	ret, err := s.table.LimitedQueryOnIndex(atc, "LSI", 5)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), 5, len(ret))
		assert.Equal(s.T(), "0", ret[0]["TestRangeKey"].Value)
	}
}

type ScanSuite struct {
	suite.Suite
	DynamoDBTest

	numOfRecords int
}

func (s *ScanSuite) SetupSuite() {
	s.TableDescription = dynamodb.TableDescription{
		TableName: "DynamoDBTestScan",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", "S"},
			dynamodb.AttributeDefinition{"TestRangeKey", "N"},
		},
		KeySchema: []dynamodb.KeySchema{
			dynamodb.KeySchema{"TestHashKey", "HASH"},
			dynamodb.KeySchema{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 10,
		},
	}
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB(s.T())
}

func (s *ScanSuite) createDummy() {
	// Create dummy records
	for i := 0; i < 100; i++ {
		ai := strconv.Itoa(i)
		attrs := []dynamodb.Attribute{
			*dynamodb.NewNumericAttribute("Attr", ai),
		}
		if ok, err := s.table.PutItem("HashKeyVal"+ai, ai, attrs); !ok {
			s.T().Fatal(err)
		}
	}
}

func (s *ScanSuite) TestScan() {
	s.createDummy()
	attrs, err := s.table.Scan(nil)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), len(attrs), s.numOfRecords)
	}
}

func (s *ScanSuite) TestScanFilter() {
	s.createDummy()
	ac := []dynamodb.AttributeComparison{
		dynamodb.AttributeComparison{
			AttributeName: "Attr",
			AttributeValueList: []dynamodb.Attribute{
				dynamodb.Attribute{
					Type:  "N",
					Value: "50",
				},
			},
			ComparisonOperator: "GE",
		},
	}
	attrs, err := s.table.Scan(ac)
	if assert.NoError(s.T(), err) {
		assert.Equal(s.T(), len(attrs), 50)
		for i := range attrs {
			ia, err := strconv.Atoi(attrs[i]["TestRangeKey"].Value)
			assert.NoError(s.T(), err)
			assert.True(s.T(), ia >= 50)
		}
	}
}

func TestTable(t *testing.T) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	suite.Run(t, new(TableSuite))
	suite.Run(t, new(TableGSISuite))
}

func TestScan(t *testing.T) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	suite.Run(t, new(ScanSuite))
}

func TestQuery(t *testing.T) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	suite.Run(t, new(QuerySuite))
	suite.Run(t, new(QueryOnIndexSuite))
}

func TestItem(t *testing.T) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	suite.Run(t, new(ItemSuite))
	suite.Run(t, &ItemSuite{WithRange: true, TableName: "DynamoDBTestItemRange"})
}
