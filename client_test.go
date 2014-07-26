package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nabeken/goamz-dynamodb"
)

func newTestTable(name string) *dynamodb.Table {
	return &dynamodb.Table{
		Name: name,
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
}

type ClientTestSuite struct {
	suite.Suite
	DynamoDBCommonSuite

	items map[string]dynamodb.AttributeValue
}

func (s *ClientTestSuite) SetupSuite() {
	s.t = s.T()
	s.Table = newTestTable("DynamoDBTest")
	s.items = dynamodb.Item{
		"TestHashKey":  dynamodb.NewString("HashKeyVal"),
		"TestRangeKey": dynamodb.NewNumber(1),
	}
	s.CreateNewTable = true
	s.SetupDB()
}

func (s *ClientTestSuite) putTestItem() {
	_, err := s.c.PutItem(s.Table.Name, s.items, nil)
	if err != nil {
		s.T().Fatal(err)
	}
}

func (s *ClientTestSuite) addAttribute(key string, attr dynamodb.AttributeValue) error {
	items := dynamodb.Item{}
	for k := range s.items {
		items[k] = s.items[k]
	}
	items[key] = attr
	_, err := s.c.PutItem(s.Table.Name, items, nil)
	return err
}

func (s *ClientTestSuite) TestListTables() {
	ret, err := s.c.ListTables(nil)
	if err != nil {
		s.T().Fatal(err)
	}
	s.Equal(len(ret.TableNames), 1)
	s.True(findTableByName(ret.TableNames, s.Table.Name))
}

func (s *ClientTestSuite) TestGetItem() {
	s.putTestItem()
	ret, err := s.c.GetItem(s.Table.Name, s.items, nil)
	if err != nil {
		s.T().Fatal(err)
	}
	s.Equal(ret.Item["TestHashKey"].Data[0], "HashKeyVal")
	s.Equal(ret.Item["TestRangeKey"].Data[0], "1")
}

func (s *ClientTestSuite) TestPutUpdateItem() {
	s.putTestItem()

	attr := dynamodb.NewNumber(1)
	if err := s.addAttribute("ATTR", attr); err != nil {
		s.T().Fatal(err)
	}

	uiro := &dynamodb.UpdateItemOption{
		AttributeUpdates: map[string]dynamodb.AttributeUpdate{
			"ATTR": dynamodb.AttributeUpdate{
				Action: dynamodb.ActionAdd,
				Value:  dynamodb.NewNumber(1),
			},
		},
	}
	if _, err := s.c.UpdateItem(s.Table.Name, s.items, uiro); err != nil {
		s.T().Fatal(err)
	}

	ret, err := s.c.GetItem(s.Table.Name, s.items, nil)
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
	s.Table = &dynamodb.Table{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"UserId", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"OSType", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"IMSI", dynamodb.TypeString},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"UserId", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"OSType", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  5,
			WriteCapacityUnits: 5,
		},
		Name: "DynamoDBTestGSI",
	}
	s.TableOption = &dynamodb.TableOption{
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
	}

	s.CreateNewTable = true
	s.SetupDB()
}

func (s *ClientGSITestSuite) TestDescribeTable() {
	td, err := s.c.DescribeTable(s.Table.Name)
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(len(td.Table.GlobalSecondaryIndexes), 1)
	s.Equal("IMSIIndex", td.Table.GlobalSecondaryIndexes[0].IndexName)
}

func (s *ClientGSITestSuite) TestUpdateTable() {
	utro := &dynamodb.UpdateTableOption{
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
	}
	_, err := s.c.UpdateTable(s.Table.Name, utro)

	describe := func() (*dynamodb.TableDescription, error) {
		td, err := s.c.DescribeTable(s.Table.Name)
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
	s.Table = newTestTable("DynamoDBTestScan")
	s.CreateNewTable = true
	s.numOfRecords = 100
	s.SetupDB()
}

func (s *ScanTestSuite) createDummy() {
	// Create dummy records
	for i := 0; i < s.numOfRecords; i++ {
		ai := strconv.Itoa(i)
		items := dynamodb.Item{
			"TestHashKey":  dynamodb.NewString("HashKeyVal" + ai),
			"TestRangeKey": dynamodb.NewNumber(i),
			"Attr":         dynamodb.NewNumber(i),
		}
		_, err := s.c.PutItem(s.Table.Name, items, nil)
		if err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *ScanTestSuite) TestScan() {
	s.createDummy()
	ret, err := s.c.Scan(s.Table.Name, nil)
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

	sro := &dynamodb.ScanOption{
		ScanFilter: sf,
	}
	ret, err := s.c.Scan(s.Table.Name, sro)
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
	s.Table = newTestTable("DynamoDBTestQuery")
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
		_, err := s.c.PutItem(s.Table.Name, items, nil)
		if err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *QueryTestSuite) TestQuery() {
	s.createDummy()
	kc := &dynamodb.KeyConditions{
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
	ret, err := s.c.Query(s.Table.Name, kc, nil)
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(1, ret.Count)
	s.Equal("0", string(ret.Items[0]["TestRangeKey"].Data[0]))
}

func (s *QueryTestSuite) TestLimitedQuery() {
	s.createDummy()
	kc := &dynamodb.KeyConditions{
		"TestHashKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewString("HashKeyVal"),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
	}

	qro := &dynamodb.QueryOption{
		Limit: 1,
	}
	ret, err := s.c.Query(s.Table.Name, kc, qro)
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
	s.Table = &dynamodb.Table{
		Name: "DynamoDBTestQueryOnIndex",
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
	}
	s.TableOption = &dynamodb.TableOption{
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
		_, err := s.c.PutItem(s.Table.Name, items, nil)
		if err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *QueryOnIndexSuite) TestQueryOnIndex() {
	s.createDummy()
	kc := &dynamodb.KeyConditions{
		"GSIKey": dynamodb.Condition{
			AttributeValueList: []dynamodb.AttributeValue{
				dynamodb.NewNumber(80),
			},
			ComparisonOperator: dynamodb.CmpOpEQ,
		},
	}
	qro := &dynamodb.QueryOption{
		IndexName: "GSI",
	}
	ret, err := s.c.Query(s.Table.Name, kc, qro)
	if !s.NoError(err) {
		s.T().FailNow()
	}
	s.Equal(1, ret.Count)
	s.Equal("80", ret.Items[0]["TestRangeKey"].Data[0])
}

func (s *QueryOnIndexSuite) TestLimitedQueryOnIndex() {
	s.createDummy()
	kc := &dynamodb.KeyConditions{
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

	qro := &dynamodb.QueryOption{
		IndexName: "LSI",
		Limit:     5,
	}
	ret, err := s.c.Query(s.Table.Name, kc, qro)
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
	s.Table = newTestTable("DynamoDBTestBatch")
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
		_, err := s.c.PutItem(s.Table.Name, items, nil)
		if err != nil {
			s.T().Fatal(err)
		}
	}
}

func (s *BatchTestSuite) TestBatchGetItem() {
	s.createDummy()
	items := map[string]dynamodb.KeysAndAttributes{
		s.Table.Name: dynamodb.KeysAndAttributes{
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
	}
	ret, err := s.c.BatchGetItem(items, nil)
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
	items := map[string][]dynamodb.WriteRequest{
		s.Table.Name: wr,
	}
	ret, err := s.c.BatchWriteItem(items, nil)
	if !s.NoError(err) {
		s.T().Fatal()
	}

	// No unprocessed item
	s.Equal(0, len(ret.UnprocessedItems[s.Table.Name]))

	sret, serr := s.c.Scan(s.Table.Name, nil)
	if !s.NoError(serr) {
		s.T().Fatal()
	}
	s.Equal(90, sret.Count)
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
