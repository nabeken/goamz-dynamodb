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

type DynamoDBTest struct {
	TableDescription dynamodb.TableDescription
	CreateNewTable   bool

	server *dynamodb.Server
	region aws.Region
	table  *dynamodb.Table
	t      *testing.T
}

// TearDownSuite implements suite.TearDownAllSuite interface.
func (dt *DynamoDBTest) TearDownSuite() {
	// Ensure that the table does not exist
	dt.DeleteTable(dt.t)
}

// TearDownTest implements suite.TearDownTestSuite interface.
func (dt *DynamoDBTest) TearDownTest() {
	dt.DeleteAllItems(dt.t)
}

// DeleteAllItems deletes all items in the table
func (dt *DynamoDBTest) DeleteAllItems(t *testing.T) {
	pk, err := dt.TableDescription.BuildPrimaryKey()
	if err != nil {
		t.Fatal(err)
	}

	attrs, err := dt.table.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range attrs {
		key := &dynamodb.Key{
			HashKey: a[pk.KeyAttribute.Name].Value,
		}
		if pk.HasRange() {
			key.RangeKey = a[pk.RangeAttribute.Name].Value
		}
		if ok, err := dt.table.DeleteItem(key); !ok {
			t.Fatal(err)
		}
	}
}

func (dt *DynamoDBTest) CreateTable(t *testing.T) {
	status, err := dt.server.CreateTable(dt.TableDescription)
	if err != nil {
		dt.t.Fatal(err)
	}
	if status != "ACTIVE" && status != "CREATING" {
		dt.t.Error("Expect status to be ACTIVE or CREATING")
	}

	dt.WaitUntilStatus(dt.t, "ACTIVE")
}

func (dt *DynamoDBTest) DeleteTable(t *testing.T) {
	// check whether the table exists
	if tables, err := dt.server.ListTables(); err != nil {
		t.Fatal(err)
	} else {
		if !findTableByName(tables, dt.TableDescription.TableName) {
			return
		}
	}

	// Delete the table and wait
	if _, err := dt.server.DeleteTable(dt.TableDescription); err != nil {
		t.Fatal(err)
	}

	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		tables, err := dt.server.ListTables()
		if err != nil {
			t.Fatal(err)
		}
		if findTableByName(tables, dt.TableDescription.TableName) {
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
		t.Error("Expect the table to be deleted but timed out")
	}
}

func (dt *DynamoDBTest) WaitUntilStatus(t *testing.T, status string) {
	// We should wait until the table is in specified status because a real DynamoDB has some delay for ready
	timeoutChan := time.After(timeout)
	done := handleAction(func(done chan struct{}) {
		desc, err := dt.table.DescribeTable()
		if err != nil {
			t.Fatal(err)
		}
		if desc.TableStatus == status {
			close(done)
		}
		time.Sleep(5 * time.Second)
	})
	select {
	case <-done:
		break
	case <-timeoutChan:
		close(done)
		t.Errorf("Expect a status to be %s, but timed out", status)
	}
}

func (dt *DynamoDBTest) SetupDB(t *testing.T) {
	if !*integration {
		t.Skip("Integration tests are disabled")
	}

	t.Logf("Performing Integration tests on %s...", *provider)

	var auth aws.Auth
	if *provider == "amazon" {
		t.Log("Using REAL AMAZON SERVER")
		awsAuth, err := aws.EnvAuth()
		if err != nil {
			log.Fatal(err)
		}
		auth = awsAuth
	} else {
		auth = dummyAuth
	}
	dt.server = &dynamodb.Server{auth, dummyRegion[*provider]}
	pk, err := dt.TableDescription.BuildPrimaryKey()
	if err != nil {
		t.Skip(err.Error())
	}

	dt.table = dt.server.NewTable(dt.TableDescription.TableName, pk)
	dt.t = t
	// Ensure that the table does not exist
	dt.DeleteTable(t)

	if dt.CreateNewTable {
		dt.CreateTable(t)
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
