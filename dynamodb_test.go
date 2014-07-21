package dynamodb_test

import (
	"flag"
	"log"
	"testing"
	"time"

	"github.com/crowdmob/goamz/aws"

	"github.com/nabeken/goamz-dynamodb"
)

const timeout = 3 * time.Minute

var (
	integration = flag.Bool("integration", false, "Enable integration tests against DynamoDB server")
	provider    = flag.String("provider", "local", "Specify a DynamoDB provider. Default: local. [local|dynalite|amazon]")
)

var (
	dummyRegion = map[string]aws.Region{
		"local":    aws.Region{DynamoDBEndpoint: "http://127.0.0.1:8000"},
		"dynalite": aws.Region{DynamoDBEndpoint: "http://127.0.0.1:4567"},
		"amazon":   aws.USEast,
	}
	dummyAuth = aws.Auth{AccessKey: "DUMMY_KEY", SecretKey: "DUMMY_SECRET"}
)

type actionHandler func(done chan struct{})

func handleAction(action actionHandler) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				action(done)
			}
		}
	}()
	return done
}

type DynamoDBCommonSuite struct {
	CreateTableRequest *dynamodb.CreateTableRequest
	CreateNewTable     bool

	c *dynamodb.Client
	t *testing.T
}

// TearDownSuite implements suite.TearDownAllSuite interface.
func (s *DynamoDBCommonSuite) TearDownSuite() {
	// Ensure that the table does not exist
	s.DeleteTable()
}

// TearDownTest implements suite.TearDownTestSuite interface.
func (s *DynamoDBCommonSuite) TearDownTest() {
	s.DeleteAllItems()
}

// DeleteAllItems deletes all items in the table
func (s *DynamoDBCommonSuite) DeleteAllItems() {
	ret, err := s.c.Scan(&dynamodb.ScanRequest{TableName: s.CreateTableRequest.TableName})
	if err != nil {
		s.t.Error(err)
		return
	}
	if ret.Count == 0 {
		return
	}

	dir := &dynamodb.DeleteItemRequest{
		Key:       make(map[string]dynamodb.AttributeValue),
		TableName: s.CreateTableRequest.TableName,
	}
	for _, ks := range s.CreateTableRequest.KeySchema {
		for i := range ret.Items {
			if v, ok := ret.Items[i][ks.AttributeName]; ok {
				dir.Key[ks.AttributeName] = v
			}
		}
	}
	if _, err := s.c.DeleteItem(dir); err != nil {
		s.t.Error(err)
		return
	}
}

func (s *DynamoDBCommonSuite) CreateTable() {
	_, cerr := s.c.CreateTable(s.CreateTableRequest)
	if cerr != nil {
		s.t.Error(cerr)
		return
	}
	s.WaitUntilStatus(dynamodb.TableStatusActive)
}

func (s *DynamoDBCommonSuite) DeleteTable() {
	// check whether the table exists
	if ret, err := s.c.ListTables(&dynamodb.ListTablesRequest{}); err != nil {
		s.t.Error(err)
		return
	} else {
		if !findTableByName(ret.TableNames, s.CreateTableRequest.TableName) {
			return
		}
	}

	// Delete the table and wait
	if _, err := s.c.DeleteTable(
		&dynamodb.DeleteTableRequest{s.CreateTableRequest.TableName}); err != nil {
		s.t.Error(err)
		return
	}

	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		ret, err := s.c.ListTables(&dynamodb.ListTablesRequest{})
		if err != nil {
			s.t.Error(err)
			close(done)
			return
		}
		if findTableByName(ret.TableNames, s.CreateTableRequest.TableName) {
			time.Sleep(5 * time.Second)
		} else {
			close(done)
		}
	})

	select {
	case <-done:
		break
	case <-timeoutChan:
		close(done)
		s.t.Error("Expect the table to be deleted but timed out")
	}
}

func (s *DynamoDBCommonSuite) WaitUntilStatus(status dynamodb.TableStatus) {
	// We should wait until the table is in specified status because a real DynamoDB has some delay for ready
	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		desc, err := s.c.DescribeTable(
			&dynamodb.DescribeTableRequest{s.CreateTableRequest.TableName})
		if err != nil {
			s.t.Error(err)
			close(done)
			return
		}
		if desc.Table.TableStatus == status {
			close(done)
		} else {
			time.Sleep(5 * time.Second)
		}
	})
	select {
	case <-done:
		break
	case <-timeoutChan:
		close(done)
		s.t.Errorf("Expect a status to be %s, but timed out", status)
	}
}

func (s *DynamoDBCommonSuite) SetupDB() {
	if !*integration {
		s.t.Skip("Integration tests are disabled")
	}

	s.t.Logf("Performing Integration tests on %s...", *provider)

	var auth aws.Auth
	if *provider == "amazon" {
		s.t.Log("Using REAL AMAZON SERVER")
		awsAuth, err := aws.EnvAuth()
		if err != nil {
			log.Fatal(err)
		}
		auth = awsAuth
	} else {
		auth = dummyAuth
	}
	s.c = &dynamodb.Client{
		Auth:   auth,
		Region: dummyRegion[*provider],
	}
	// Ensure that the table does not exist
	s.DeleteTable()

	if s.CreateNewTable {
		s.CreateTable()
	}
}

func findTableByName(tables []string, name string) bool {
	for _, t := range tables {
		if t == name {
			return true
		}
	}
	return false
}
