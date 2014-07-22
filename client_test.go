package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nabeken/goamz-dynamodb"
)

type ClientTestSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	items map[string]dynamodb.AttributeValue
}

func (s *ClientTestSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		TableName: "DynamoDBTest",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"TestRangeKey", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"TestHashKey", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"TestRangeKey", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
	}
	s.items = map[string]dynamodb.AttributeValue{
		"TestHashKey":  dynamodb.NewString("HashKeyVal"),
		"TestRangeKey": dynamodb.NewNumber(1),
	}
	s.CreateNewTable = true
	s.SetupDB()
}

func (s *ClientTestSuite) putTestItem() error {
	pir := &dynamodb.PutItemRequest{
		Item:      s.items,
		TableName: s.CreateTableRequest.TableName,
	}
	_, err := s.c.PutItem(pir)
	return err
}

func (s *ClientTestSuite) addAttribute(key string, attr dynamodb.AttributeValue) error {
	items := map[string]dynamodb.AttributeValue{}
	for k := range s.items {
		items[k] = s.items[k]
	}
	items[key] = attr
	pir := &dynamodb.PutItemRequest{
		Item:      items,
		TableName: s.CreateTableRequest.TableName,
	}
	_, err := s.c.PutItem(pir)
	return err
}

func (s *ClientTestSuite) TestListTables() {
	ret, err := s.c.ListTables(&dynamodb.ListTablesRequest{})
	if err != nil {
		s.T().Fatal(err)
	}
	s.Equal(len(ret.TableNames), 1)
	s.True(findTableByName(ret.TableNames, s.CreateTableRequest.TableName))
}

func (s *ClientTestSuite) TestGetItem() {
	if err := s.putTestItem(); err != nil {
		s.T().Fatal(err)
	}

	gir := &dynamodb.GetItemRequest{
		Key:       s.items,
		TableName: s.CreateTableRequest.TableName,
	}
	ret, err := s.c.GetItem(gir)
	if err != nil {
		s.T().Fatal(err)
	}
	s.Equal(ret.Item["TestHashKey"].Data[0], "HashKeyVal")
	s.Equal(ret.Item["TestRangeKey"].Data[0], "1")
}

func (s *ClientTestSuite) TestPutUpdateItem() {
	if err := s.putTestItem(); err != nil {
		s.T().Fatal(err)
	}

	attr := dynamodb.NewNumber(1)
	if err := s.addAttribute("ATTR", attr); err != nil {
		s.T().Fatal(err)
	}

	uir := &dynamodb.UpdateItemRequest{
		AttributeUpdates: map[string]dynamodb.AttributeUpdate{
			"ATTR": dynamodb.AttributeUpdate{
				Action: dynamodb.ActionAdd,
				Value:  dynamodb.NewNumber(1),
			},
		},
		Key:       s.items,
		TableName: s.CreateTableRequest.TableName,
	}
	if _, err := s.c.UpdateItem(uir); err != nil {
		s.T().Fatal(err)
	}

	gir := &dynamodb.GetItemRequest{
		Key:       s.items,
		TableName: s.CreateTableRequest.TableName,
	}
	ret, err := s.c.GetItem(gir)
	if err != nil {
		s.T().Fatal(err)
	}
	s.Equal(ret.Item["TestHashKey"].Data[0], "HashKeyVal")
	s.Equal(ret.Item["TestRangeKey"].Data[0], "1")
	s.Equal(ret.Item["ATTR"].Data[0], "2")
}

type ClientGSITestSuite struct {
	suite.Suite
	DynamoDBCommonSuite
}

func (s *ClientGSITestSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"UserId", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"OSType", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"IMSI", dynamodb.TypeString},
		},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndex{
			dynamodb.GlobalSecondaryIndex{
				IndexName: "IMSIIndex",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{"IMSI", dynamodb.KeyTypeHash},
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
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"UserId", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"OSType", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  5,
			WriteCapacityUnits: 5,
		},
		TableName: "DynamoDBTestGSI",
	}

	s.CreateNewTable = true
	s.SetupDB()
}

func (s *ClientGSITestSuite) TestDescribeTable() {
	td, err := s.c.DescribeTable(
		&dynamodb.DescribeTableRequest{TableName: s.CreateTableRequest.TableName})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(len(td.Table.GlobalSecondaryIndexes), 1)
	s.Equal("IMSIIndex", td.Table.GlobalSecondaryIndexes[0].IndexName)
}

func (s *ClientGSITestSuite) TestUpdateTable() {
	_, err := s.c.UpdateTable(&dynamodb.UpdateTableRequest{
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
		TableName: s.CreateTableRequest.TableName,
	})

	describe := func() (*dynamodb.TableDescription, error) {
		td, err := s.c.DescribeTable(
			&dynamodb.DescribeTableRequest{TableName: s.CreateTableRequest.TableName})
		if err != nil {
			return nil, err
		}
		return &td.Table, nil
	}

	if !s.NoError(err) {
		s.T().Error()
		return
	}
	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		td, terr := describe()
		if s.NoError(terr) {
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
		td, terr := describe()
		if !s.NoError(terr) {
			s.T().Error()
			return
		}
		s.Equal(10, td.ProvisionedThroughput.ReadCapacityUnits)
		s.Equal(10, td.ProvisionedThroughput.WriteCapacityUnits)
		s.Equal(10, td.GlobalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
		s.Equal(10, td.GlobalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
	case <-timeoutChan:
		close(done)
		s.T().Errorf("Expect ProvisionedThroughput to be changed, but timed out")
	}
}

type ScanTestSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	numOfRecords int
}

func (s *ScanTestSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		TableName: "DynamoDBTestScan",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"TestRangeKey", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"TestHashKey", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"TestRangeKey", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 10,
		},
	}
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB()
}

func (s *ScanTestSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		items := map[string]dynamodb.AttributeValue{
			"TestHashKey":  dynamodb.NewString("HashKeyVal" + ai),
			"TestRangeKey": dynamodb.NewNumber(i),
			"Attr":         dynamodb.NewNumber(i),
		}
		pir := &dynamodb.PutItemRequest{
			Item:      items,
			TableName: s.CreateTableRequest.TableName,
		}
		if _, err := s.c.PutItem(pir); err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *ScanTestSuite) TestScan() {
	s.createDummy()
	ret, err := s.c.Scan(&dynamodb.ScanRequest{TableName: s.CreateTableRequest.TableName})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(ret.Count, s.numOfRecords)
}

func (s *ScanTestSuite) TestScanFilter() {
	s.createDummy()
	sf := dynamodb.ScanFilter{
		"Attr": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewNumber(50),
			},
			ComparisonOperator: dynamodb.CmpOpGE,
		},
	}

	ret, err := s.c.Scan(&dynamodb.ScanRequest{
		ScanFilter: sf,
		TableName:  s.CreateTableRequest.TableName,
	})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(ret.Count, 50)
	for i := range ret.Items {
		ia, err := strconv.Atoi(string(ret.Items[i]["TestRangeKey"].Data[0]))
		s.NoError(err)
		s.True(ia >= 50)
	}
}

type QueryTestSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	numOfRecords int
}

func (s *QueryTestSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		TableName: "DynamoDBTestQuery",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"TestRangeKey", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"TestHashKey", "HASH"},
			dynamodb.KeySchemaElement{"TestRangeKey", "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB()
}

func (s *QueryTestSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		items := map[string]dynamodb.AttributeValue{
			"TestHashKey":  dynamodb.NewString("HashKeyVal"),
			"TestRangeKey": dynamodb.NewNumber(i),
			"Attr":         dynamodb.NewNumber(i),
		}
		pir := &dynamodb.PutItemRequest{
			Item:      items,
			TableName: s.CreateTableRequest.TableName,
		}
		if _, err := s.c.PutItem(pir); err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *QueryTestSuite) TestQuery() {
	s.createDummy()
	kc := dynamodb.KeyConditions{
		"TestHashKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewString("HashKeyVal"),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
		"TestRangeKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewNumber(1),
			},
			ComparisonOperator: dynamodb.CmpOpLT,
		},
	}
	ret, err := s.c.Query(&dynamodb.QueryRequest{
		KeyConditions: kc,
		TableName:     s.CreateTableRequest.TableName,
	})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(1, ret.Count)
	s.Equal("0", string(ret.Items[0]["TestRangeKey"].Data[0]))
}

func (s *QueryTestSuite) TestLimitedQuery() {
	s.createDummy()
	kc := dynamodb.KeyConditions{
		"TestHashKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewString("HashKeyVal"),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
	}

	ret, err := s.c.Query(&dynamodb.QueryRequest{
		KeyConditions: kc,
		Limit:         1,
		TableName:     s.CreateTableRequest.TableName,
	})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(1, ret.Count)
	s.Equal("0", string(ret.Items[0]["TestRangeKey"].Data[0]))
}

type QueryOnIndexSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	numOfRecords int
}

func (s *QueryOnIndexSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		TableName: "DynamoDBTestQueryOnIndex",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"TestRangeKey", dynamodb.TypeNumber},
			dynamodb.AttributeDefinition{"LSIKey", dynamodb.TypeNumber},
			dynamodb.AttributeDefinition{"GSIKey", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"TestHashKey", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"TestRangeKey", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndex{
			dynamodb.GlobalSecondaryIndex{
				IndexName: "GSI",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{"GSIKey", "HASH"},
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
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{"TestHashKey", dynamodb.KeyTypeHash},
					dynamodb.KeySchemaElement{"LSIKey", dynamodb.KeyTypeRange},
				},
				Projection: dynamodb.Projection{
					ProjectionType: "KEYS_ONLY",
				},
			},
		},
	}

	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB()
}

func (s *QueryOnIndexSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		items := map[string]dynamodb.AttributeValue{
			"TestHashKey":  dynamodb.NewString("HashKeyVal"),
			"TestRangeKey": dynamodb.NewNumber(i),
			"GSIKey":       dynamodb.NewNumber(i),
			"LSIKey":       dynamodb.NewNumber(i),
		}
		pir := &dynamodb.PutItemRequest{
			Item:      items,
			TableName: s.CreateTableRequest.TableName,
		}
		if _, err := s.c.PutItem(pir); err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *QueryOnIndexSuite) TestQueryOnIndex() {
	s.createDummy()
	kc := dynamodb.KeyConditions{
		"GSIKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewNumber(80),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
	}
	ret, err := s.c.Query(&dynamodb.QueryRequest{
		IndexName:     "GSI",
		KeyConditions: kc,
		TableName:     s.CreateTableRequest.TableName,
	})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(1, ret.Count)
	s.Equal("80", ret.Items[0]["TestRangeKey"].Data[0])
}

func (s *QueryOnIndexSuite) TestLimitedQueryOnIndex() {
	s.createDummy()
	kc := dynamodb.KeyConditions{
		"TestHashKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewString("HashKeyVal"),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
		"LSIKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewNumber(10),
			},
			ComparisonOperator: dynamodb.CmpOpLT,
		},
	}

	ret, err := s.c.Query(&dynamodb.QueryRequest{
		IndexName:     "LSI",
		KeyConditions: kc,
		Limit:         5,
		TableName:     s.CreateTableRequest.TableName,
	})
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(5, ret.Count)
	s.Equal("0", ret.Items[0]["TestRangeKey"].Data[0])
}

type BatchTestSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	numOfRecords int
}

func (s *BatchTestSuite) SetupSuite() {
	s.t = s.T()
	s.CreateTableRequest = &dynamodb.CreateTableRequest{
		TableName: "DynamoDBTestBatch",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"TestHashKey", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"TestRangeKey", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"TestHashKey", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"TestRangeKey", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB()
}

func (s *BatchTestSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		items := map[string]dynamodb.AttributeValue{
			"TestHashKey":  dynamodb.NewString("HashKeyVal" + ai),
			"TestRangeKey": dynamodb.NewNumber(i),
			"Attr":         dynamodb.NewNumber(i),
		}
		pir := &dynamodb.PutItemRequest{
			Item:      items,
			TableName: s.CreateTableRequest.TableName,
		}
		if _, err := s.c.PutItem(pir); err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *BatchTestSuite) TestBatchGetItem() {
	s.createDummy()
	bgir := &dynamodb.BatchGetItemRequest{
		RequestItems: map[string]dynamodb.KeysAndAttributes{
			s.CreateTableRequest.TableName: dynamodb.KeysAndAttributes{
				Keys: []map[string]dynamodb.AttributeValue{
					map[string]dynamodb.AttributeValue{
						"TestHashKey":  dynamodb.NewString("HashKeyVal0"),
						"TestRangeKey": dynamodb.NewNumber(0),
					},
					map[string]dynamodb.AttributeValue{
						"TestHashKey":  dynamodb.NewString("HashKeyVal1"),
						"TestRangeKey": dynamodb.NewNumber(1),
					},
					map[string]dynamodb.AttributeValue{
						"TestHashKey":  dynamodb.NewString("HashKeyVal2"),
						"TestRangeKey": dynamodb.NewNumber(2),
					},
				},
			},
		},
	}
	ret, err := s.c.BatchGetItem(bgir)
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(len(ret.Responses["DynamoDBTestBatch"]), 3)
	for i := range ret.Responses["DynamoDBTest"] {
		ia, err := strconv.Atoi(string(ret.Responses["DynamoDBTestBatch"][i]["TestRangeKey"].Data[0]))
		s.NoError(err)
		s.True(ia <= 2)
	}
}

func (s *BatchTestSuite) TestBatchWrite() {
	s.createDummy()

	wr := []dynamodb.WriteRequest{}
	for i := 0; i < 10; i++ {
		ai := strconv.Itoa(i)
		wr = append(wr, dynamodb.WriteRequest{
			DeleteRequest: dynamodb.DeleteRequest{
				Key: map[string]dynamodb.AttributeValue{
					"TestHashKey":  dynamodb.NewString("HashKeyVal" + ai),
					"TestRangeKey": dynamodb.NewNumber(i),
				},
			},
		})
	}
	ret, err := s.c.BatchWriteItem(&dynamodb.BatchWriteItemRequest{
		RequestItems: map[string][]dynamodb.WriteRequest{
			s.CreateTableRequest.TableName: wr,
		},
	})
	if !s.NoError(err) {
		s.T().Fatal()
	}

	// No unprocessed item
	s.Equal(0, len(ret.UnprocessedItems[s.CreateTableRequest.TableName]))

	scanRet, err := s.c.Scan(&dynamodb.ScanRequest{TableName: s.CreateTableRequest.TableName})
	if !s.NoError(err) {
		s.T().Fatal()
	}
	s.Equal(90, scanRet.Count)
}

func TestBatch(t *testing.T) {
	doIntegrationTest(t, new(BatchTestSuite))
}

func TestQuery(t *testing.T) {
	doIntegrationTest(t, new(QueryTestSuite), new(QueryOnIndexSuite))
}

func TestScan(t *testing.T) {
	doIntegrationTest(t, new(ScanTestSuite))
}

func TestClientTestSuite(t *testing.T) {
	doIntegrationTest(t, new(ClientTestSuite), new(ClientGSITestSuite))
}
