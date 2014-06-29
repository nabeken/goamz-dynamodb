package dynamodb_test

import (
	"encoding/json"
	"testing"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type QueryBuilderSuite struct {
	suite.Suite

	server *dynamodb.Server
}

func (s *QueryBuilderSuite) SetupSuite() {
	s.server = &dynamodb.Server{dummyAuth, dummyRegion["local"]}
}

func (s *QueryBuilderSuite) TestEmptyQuery() {
	q := dynamodb.NewEmptyQuery()
	queryString := q.String()
	assert.Equal(s.T(), queryString, "{}")
}

type TestBatchWrite struct {
	RequestItems map[string][]map[string]map[string]map[string]map[string]string
}

func (s *QueryBuilderSuite) TestAddWriteRequestItems() {
	primary := dynamodb.NewStringAttribute("WidgetFoo", "")
	secondary := dynamodb.NewNumericAttribute("Created", "")
	key := dynamodb.PrimaryKey{primary, secondary}
	table := s.server.NewTable("FooData", key)

	primary2 := dynamodb.NewStringAttribute("TestHashKey", "")
	secondary2 := dynamodb.NewNumericAttribute("TestRangeKey", "")
	key2 := dynamodb.PrimaryKey{primary2, secondary2}
	table2 := s.server.NewTable("TestTable", key2)

	q := dynamodb.NewEmptyQuery()

	attribute1 := dynamodb.NewNumericAttribute("testing", "4")
	attribute2 := dynamodb.NewNumericAttribute("testingbatch", "2111")
	attribute3 := dynamodb.NewStringAttribute("testingstrbatch", "mystr")
	item1 := []dynamodb.Attribute{*attribute1, *attribute2, *attribute3}

	attribute4 := dynamodb.NewNumericAttribute("testing", "444")
	attribute5 := dynamodb.NewNumericAttribute("testingbatch", "93748249272")
	attribute6 := dynamodb.NewStringAttribute("testingstrbatch", "myotherstr")
	item2 := []dynamodb.Attribute{*attribute4, *attribute5, *attribute6}

	attributeDel1 := dynamodb.NewStringAttribute("TestHashKeyDel", "DelKey")
	attributeDel2 := dynamodb.NewNumericAttribute("TestRangeKeyDel", "7777777")
	itemDel := []dynamodb.Attribute{*attributeDel1, *attributeDel2}

	attributeTest1 := dynamodb.NewStringAttribute("TestHashKey", "MyKey")
	attributeTest2 := dynamodb.NewNumericAttribute("TestRangeKey", "0193820384293")
	itemTest := []dynamodb.Attribute{*attributeTest1, *attributeTest2}

	tableItems := map[*dynamodb.Table]map[string][][]dynamodb.Attribute{}
	actionItems := make(map[string][][]dynamodb.Attribute)
	actionItems["Put"] = [][]dynamodb.Attribute{item1, item2}
	actionItems["Delete"] = [][]dynamodb.Attribute{itemDel}
	tableItems[table] = actionItems

	actionItems2 := make(map[string][][]dynamodb.Attribute)
	actionItems2["Put"] = [][]dynamodb.Attribute{itemTest}
	tableItems[table2] = actionItems2

	q.AddWriteRequestItems(tableItems)

	queryJson := TestBatchWrite{}
	err := json.Unmarshal([]byte(q.String()), &queryJson)
	if err != nil {
		s.T().Fatal(err)
	}

	expectedJson := TestBatchWrite{}
	err = json.Unmarshal([]byte(`
{
  "RequestItems": {
    "TestTable": [
      {
        "PutRequest": {
          "Item": {
            "TestRangeKey": {
              "N": "0193820384293"
            },
            "TestHashKey": {
              "S": "MyKey"
            }
          }
        }
      }
    ],
    "FooData": [
      {
        "DeleteRequest": {
          "Key": {
            "TestRangeKeyDel": {
              "N": "7777777"
            },
            "TestHashKeyDel": {
              "S": "DelKey"
            }
          }
        }
      },
      {
        "PutRequest": {
          "Item": {
            "testingstrbatch": {
              "S": "mystr"
            },
            "testingbatch": {
              "N": "2111"
            },
            "testing": {
              "N": "4"
            }
          }
        }
      },
      {
        "PutRequest": {
          "Item": {
            "testingstrbatch": {
              "S": "myotherstr"
            },
            "testingbatch": {
              "N": "93748249272"
            },
            "testing": {
              "N": "444"
            }
          }
        }
      }
    ]
  }
}
	`), &expectedJson)
	if err != nil {
		s.T().Fatal(err)
	}

	// very scarily... horrible because current implementation is messed with map.
	// I have no idea to compare two large maps except this way...
	// k = TableName
	for k := range queryJson.RequestItems {
		// i = index
		for i := range queryJson.RequestItems[k] {
			// a = request
			for a := range queryJson.RequestItems[k][i] {
				// v= Item/Key
				for v := range queryJson.RequestItems[k][i][a] {
					for key := range queryJson.RequestItems[k][i][a][v] {
						assert.Equal(s.T(), queryJson.RequestItems[k][i][a][v][key], expectedJson.RequestItems[k][i][a][v][key])
					}
				}
			}
		}
	}
}

func (s *QueryBuilderSuite) TestAddExpectedQuery() {
	primary := dynamodb.NewStringAttribute("domain", "")
	key := dynamodb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := dynamodb.NewQuery(table)
	q.AddKey(table, &dynamodb.Key{HashKey: "test"})

	expected := []dynamodb.Attribute{
		*dynamodb.NewStringAttribute("domain", "expectedTest").SetExists(true),
		*dynamodb.NewStringAttribute("testKey", "").SetExists(false),
	}
	q.AddExpected(expected)

	queryJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(q.String()), &queryJson)
	if err != nil {
		s.T().Fatal(err)
	}

	expectedJson := make(map[string]interface{})
	err = json.Unmarshal([]byte(`
	{
		"Expected": {
			"domain": {
				"Exists": true,
				"Value": {
					"S": "expectedTest"
				}
			},
			"testKey": {
				"Exists": false
			}
		},
		"Key": {
			"domain": {
				"S": "test"
			}
		},
		"TableName": "sites"
	}
	`), &expectedJson)
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), queryJson, expectedJson)
}

func (s *QueryBuilderSuite) TestGetItemQuery() {
	primary := dynamodb.NewStringAttribute("domain", "")
	key := dynamodb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := dynamodb.NewQuery(table)
	q.AddKey(table, &dynamodb.Key{HashKey: "test"})

	{
		queryJson := make(map[string]interface{})
		err := json.Unmarshal([]byte(q.String()), &queryJson)
		if err != nil {
			s.T().Fatal(err)
		}

		expectedJson := make(map[string]interface{})
		err = json.Unmarshal([]byte(`
		{
			"Key": {
				"domain": {
					"S": "test"
				}
			},
			"TableName": "sites"
		}
		`), &expectedJson)
		if err != nil {
			s.T().Fatal(err)
		}
		assert.Equal(s.T(), queryJson, expectedJson)
	}

	// Use ConsistentRead
	{
		q.ConsistentRead(true)
		queryJson := make(map[string]interface{})
		err := json.Unmarshal([]byte(q.String()), &queryJson)
		if err != nil {
			s.T().Fatal(err)
		}

		expectedJson := make(map[string]interface{})
		err = json.Unmarshal([]byte(`
		{
			"ConsistentRead": true,
			"Key": {
				"domain": {
					"S": "test"
				}
			},
			"TableName": "sites"
		}
		`), &expectedJson)
		if err != nil {
			s.T().Fatal(err)
		}
		assert.Equal(s.T(), queryJson, expectedJson)
	}
}

func (s *QueryBuilderSuite) TestUpdateQuery() {
	primary := dynamodb.NewStringAttribute("domain", "")
	rangek := dynamodb.NewNumericAttribute("time", "")
	key := dynamodb.PrimaryKey{primary, rangek}
	table := s.server.NewTable("sites", key)

	countAttribute := dynamodb.NewNumericAttribute("count", "4")
	attributes := []dynamodb.Attribute{*countAttribute}

	q := dynamodb.NewQuery(table)
	q.AddKey(table, &dynamodb.Key{HashKey: "test", RangeKey: "1234"})
	q.AddUpdates(attributes, "ADD")

	queryJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(q.String()), &queryJson)
	if err != nil {
		s.T().Fatal(err)
	}
	expectedJson := make(map[string]interface{})
	err = json.Unmarshal([]byte(`
{
	"AttributeUpdates": {
		"count": {
			"Action": "ADD",
			"Value": {
				"N": "4"
			}
		}
	},
	"Key": {
		"domain": {
			"S": "test"
		},
		"time": {
			"N": "1234"
		}
	},
	"TableName": "sites"
}
	`), &expectedJson)
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), queryJson, expectedJson)
}

func (s *QueryBuilderSuite) TestAddUpdates() {
	primary := dynamodb.NewStringAttribute("domain", "")
	key := dynamodb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := dynamodb.NewQuery(table)
	q.AddKey(table, &dynamodb.Key{HashKey: "test"})

	attr := dynamodb.NewStringSetAttribute("StringSet", []string{"str", "str2"})

	q.AddUpdates([]dynamodb.Attribute{*attr}, "ADD")

	queryJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(q.String()), &queryJson)
	if err != nil {
		s.T().Fatal(err)
	}
	expectedJson := make(map[string]interface{})
	err = json.Unmarshal([]byte(`
{
	"AttributeUpdates": {
		"StringSet": {
			"Action": "ADD",
			"Value": {
				"SS": ["str", "str2"]
			}
		}
	},
	"Key": {
		"domain": {
			"S": "test"
		}
	},
	"TableName": "sites"
}
	`), &expectedJson)
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), queryJson, expectedJson)
}

func (s *QueryBuilderSuite) TestAddKeyConditions() {
	primary := dynamodb.NewStringAttribute("domain", "")
	key := dynamodb.PrimaryKey{primary, nil}
	table := s.server.NewTable("sites", key)

	q := dynamodb.NewQuery(table)
	acs := []dynamodb.AttributeComparison{
		*dynamodb.NewStringAttributeComparison("domain", "EQ", "example.com"),
		*dynamodb.NewStringAttributeComparison("path", "EQ", "/"),
	}
	q.AddKeyConditions(acs)
	queryJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(q.String()), &queryJson)

	if err != nil {
		s.T().Fatal(err)
	}

	expectedJson := make(map[string]interface{})
	err = json.Unmarshal([]byte(`
{
  "KeyConditions": {
    "domain": {
      "AttributeValueList": [
        {
          "S": "example.com"
        }
      ],
      "ComparisonOperator": "EQ"
    },
    "path": {
      "AttributeValueList": [
        {
          "S": "/"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "TableName": "sites"
}
	`), &expectedJson)
	if err != nil {
		s.T().Fatal(err)
	}
	assert.Equal(s.T(), queryJson, expectedJson)
}

func TestQueryBuilder(t *testing.T) {
	suite.Run(t, new(QueryBuilderSuite))
}
