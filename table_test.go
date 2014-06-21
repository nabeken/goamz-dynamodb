package dynamodb_test

import (
	"fmt"

	"github.com/nabeken/goamz-dynamodb"
	"gopkg.in/check.v1"
)

type TableSuite struct {
	TableDescription dynamodb.TableDescription
	DynamoDBTest
}

func (s *TableSuite) SetUpSuite(c *check.C) {
	setUpAuth(c)
	s.DynamoDBTest.TableDescription = s.TableDescription
	s.server = &dynamodb.Server{dynamodb_auth, dynamodb_region}
	pk, err := s.TableDescription.BuildPrimaryKey()
	if err != nil {
		c.Skip(err.Error())
	}
	s.table = s.server.NewTable(s.TableDescription.TableName, pk)

	// Cleanup
	s.TearDownSuite(c)
}

var table_suite = &TableSuite{
	TableDescription: dynamodb.TableDescription{
		TableName: "DynamoDBTestMyTable",
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
	},
}

var table_suite_gsi = &TableSuite{
	TableDescription: dynamodb.TableDescription{
		TableName: "DynamoDBTestMyTable2",
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
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
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
					ReadCapacityUnits:  1,
					WriteCapacityUnits: 1,
				},
			},
		},
	},
}

func (s *TableSuite) TestCreateListTableGsi(c *check.C) {
	status, err := s.server.CreateTable(s.TableDescription)
	if err != nil {
		fmt.Printf("err %#v", err)
		c.Fatal(err)
	}
	if status != "ACTIVE" && status != "CREATING" {
		c.Error("Expect status to be ACTIVE or CREATING")
	}

	s.WaitUntilStatus(c, "ACTIVE")

	tables, err := s.server.ListTables()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(tables), check.Not(check.Equals), 0)
	c.Check(findTableByName(tables, s.TableDescription.TableName), check.Equals, true)
}

var _ = check.Suite(table_suite)
var _ = check.Suite(table_suite_gsi)
