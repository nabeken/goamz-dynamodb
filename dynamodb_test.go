package dynamodb_test

import (
	"flag"
	"log"
	"testing"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/stretchr/testify/suite"

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
	Table          *dynamodb.Table
	TableOption    *dynamodb.TableOption
	CreateNewTable bool

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
	sret, serr := s.c.Scan(s.Table.Name, nil)
	if serr != nil {
		s.t.Error(serr)
		return
	}
	if sret.Count == 0 {
		return
	}

	key := map[string]dynamodb.AttributeValue{}
	for _, ks := range s.Table.KeySchema {
		for i := range sret.Items {
			if v, ok := sret.Items[i][ks.AttributeName]; ok {
				key[ks.AttributeName] = v
			}
		}
	}
	if _, err := s.c.DeleteItem(s.Table.Name, key, nil); err != nil {
		s.t.Error(err)
		return
	}
}

func (s *DynamoDBCommonSuite) CreateTable() {
	_, err := s.c.CreateTable(s.Table, s.TableOption)
	if err != nil {
		s.t.Error(err)
		return
	}
	s.WaitUntilStatus(dynamodb.TableStatusActive)
}

func (s *DynamoDBCommonSuite) DeleteTable() {
	// check whether the table exists
	ret, err := s.c.ListTables(nil)
	if err != nil {
		s.t.Error(err)
		return
	} else {
		if !findTableByName(ret.TableNames, s.Table.Name) {
			return
		}
	}

	// Delete the table and wait
	s.c.DeleteTable(s.Table.Name)

	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		ret, err := s.c.ListTables(nil)
		if err != nil {
			s.t.Error(err)
			close(done)
			return
		}
		if findTableByName(ret.TableNames, s.Table.Name) {
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
		desc, err := s.c.DescribeTable(s.Table.Name)
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

func doIntegrationTest(t *testing.T, suites ...suite.TestingSuite) {
	if !*integration {
		t.Skip("Test against amazon not enabled.")
	}
	for i := range suites {
		suite.Run(t, suites[i])
	}
}
