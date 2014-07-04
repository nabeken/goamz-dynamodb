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

func TestDeleteItemQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "Key": {
    "DELETE_ITEM_QUERY_KEY": {
      "S": "STRING"
    }
  },
  "TableName": "DELETE_ITEM_QUERY_TABLE"
}
`)
	q := dynamodb.DeleteItemQuery{
		Key: map[string]dynamodb.AttributeValue{
			"DELETE_ITEM_QUERY_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		TableName: "DELETE_ITEM_QUERY_TABLE",
	}
	expectedQuery := dynamodb.DeleteItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestDeleteItemQuery_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "ConditionalOperator": "OR",
  "Expected": {
    "DELETE_ITEM_QUERY_KEY": {
      "Value": {
          "S": "STRING"
      },
      "Exists": true
    },
    "DELETE_ITEM_QUERY_KEY2": {
      "Exists": false
    }
  },
  "Key": {
    "DELETE_ITEM_QUERY_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD",
  "TableName": "DELETE_ITEM_QUERY_TABLE"
}
`)
	q := dynamodb.DeleteItemQuery{
		ConditionalOperator: dynamodb.CondOpOr,
		Expected: map[string]dynamodb.DeprecatedCondition{
			"DELETE_ITEM_QUERY_KEY": dynamodb.DeprecatedCondition{
				Value: dynamodb.AttributeValue{
					Type: dynamodb.TypeString,
					Data: []dynamodb.AttributeData{"STRING"},
				},
				Exists: true,
			},
			"DELETE_ITEM_QUERY_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		Key: map[string]dynamodb.AttributeValue{
			"DELETE_ITEM_QUERY_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "DELETE_ITEM_QUERY_TABLE",
	}
	expectedQuery := dynamodb.DeleteItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestGetItemQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "Key": {
    "GET_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "TableName": "GET_ITEM_TABLE"
}
`)
	q := dynamodb.GetItemQuery{
		Key: map[string]dynamodb.AttributeValue{
			"GET_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		TableName: "GET_ITEM_TABLE",
	}
	expectedQuery := dynamodb.GetItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestGetItemQuery_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "AttributesToGet": [
    "ATTR1",
    "ATTR2"
  ],
  "ConsistentRead": true,
  "Key": {
    "GET_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "TableName": "GET_ITEM_TABLE"
}
`)
	q := dynamodb.GetItemQuery{
		AttributesToGet: []string{"ATTR1", "ATTR2"},
		ConsistentRead:  true,
		Key: map[string]dynamodb.AttributeValue{
			"GET_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
		TableName:              "GET_ITEM_TABLE",
	}
	expectedQuery := dynamodb.GetItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestPutQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "Item": {
    "PUT_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "TableName": "PUT_ITEM_TABLE"
}
`)
	q := dynamodb.PutItemQuery{
		Item: map[string]dynamodb.AttributeValue{
			"PUT_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		TableName: "PUT_ITEM_TABLE",
	}
	expectedQuery := dynamodb.PutItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestPutQuery_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "ConditionalOperator": "OR",
  "Expected": {
    "PUT_ITEM_QUERY_KEY": {
      "Value": {
          "S": "STRING"
      },
      "Exists": true
    },
    "PUT_ITEM_QUERY_KEY2": {
      "Exists": false
    }
  },
  "Item": {
    "PUT_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD",
  "TableName": "PUT_ITEM_QUERY_TABLE"
}
`)
	q := dynamodb.PutItemQuery{
		ConditionalOperator: dynamodb.CondOpOr,
		Expected: map[string]dynamodb.DeprecatedCondition{
			"PUT_ITEM_QUERY_KEY": dynamodb.DeprecatedCondition{
				Value: dynamodb.AttributeValue{
					Type: dynamodb.TypeString,
					Data: []dynamodb.AttributeData{"STRING"},
				},
				Exists: true,
			},
			"PUT_ITEM_QUERY_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		Item: map[string]dynamodb.AttributeValue{
			"PUT_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "PUT_ITEM_QUERY_TABLE",
	}
	expectedQuery := dynamodb.PutItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "KeyConditions": {
    "QUERY_KEY": {
      "AttributeValueList": [
        {
          "S": "STRING"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "TableName": "QUERY_TABLE"
}
`)
	q := dynamodb.QueryQuery{
		KeyConditions: map[string]dynamodb.Condition{
			"QUERY_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.AttributeValue{
						Type: dynamodb.TypeString,
						Data: []dynamodb.AttributeData{"STRING"},
					},
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		TableName: "QUERY_TABLE",
	}
	expectedQuery := dynamodb.QueryQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestQuery_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "AttributesToGet": [
    "ATTR1",
    "ATTR2"
  ],
  "ConditionalOperator": "AND",
  "ConsistentRead": true,
  "ExclusiveStartKey": {
    "START": {
      "N": "123456789"
    }
  },
  "IndexName": "MYGSI",
  "KeyConditions": {
    "QUERY_KEY": {
      "AttributeValueList": [
        {
          "S": "STRING"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "Limit": 100,
  "QueryFilter": {
    "QUERY_KEY": {
      "AttributeValueList": [
        {
          "S": "STRING"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ScanIndexForward": true,
  "Select": "COUNT",
  "TableName": "QUERY_TABLE"
}
`)
	q := dynamodb.QueryQuery{
		AttributesToGet:     []string{"ATTR1", "ATTR2"},
		ConditionalOperator: dynamodb.CondOpAnd,
		ConsistentRead:      true,
		ExclusiveStartKey: map[string]dynamodb.AttributeValue{
			"START": dynamodb.AttributeValue{
				Type: dynamodb.TypeNumber,
				Data: []dynamodb.AttributeData{"123456789"},
			},
		},
		IndexName: "MYGSI",
		KeyConditions: map[string]dynamodb.Condition{
			"QUERY_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.AttributeValue{
						Type: dynamodb.TypeString,
						Data: []dynamodb.AttributeData{"STRING"},
					},
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		Limit: 100,
		QueryFilter: map[string]dynamodb.Condition{
			"QUERY_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.AttributeValue{
						Type: dynamodb.TypeString,
						Data: []dynamodb.AttributeData{"STRING"},
					},
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
		ScanIndexForward:       true,
		Select:                 dynamodb.SelectCount,
		TableName:              "QUERY_TABLE",
	}
	expectedQuery := dynamodb.QueryQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestScan_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "TableName": "SCAN_TABLE"
}
`)
	q := dynamodb.ScanQuery{
		TableName: "SCAN_TABLE",
	}
	expectedQuery := dynamodb.ScanQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestScan_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "AttributesToGet": [
    "ATTR1",
    "ATTR2"
  ],
  "ConditionalOperator": "AND",
  "ExclusiveStartKey": {
    "START": {
      "N": "123456789"
    }
  },
  "Limit": 100,
  "ReturnConsumedCapacity": "TOTAL",
  "ScanFilter": {
    "QUERY_KEY": {
      "AttributeValueList": [
        {
          "S": "STRING"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "Segment": 1,
  "Select": "COUNT",
  "TableName": "QUERY_TABLE",
  "TotalSegments": 100
}
`)
	q := dynamodb.ScanQuery{
		AttributesToGet:     []string{"ATTR1", "ATTR2"},
		ConditionalOperator: dynamodb.CondOpAnd,
		ExclusiveStartKey: map[string]dynamodb.AttributeValue{
			"START": dynamodb.AttributeValue{
				Type: dynamodb.TypeNumber,
				Data: []dynamodb.AttributeData{"123456789"},
			},
		},
		Limit: 100,
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
		ScanFilter: map[string]dynamodb.Condition{
			"QUERY_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.AttributeValue{
						Type: dynamodb.TypeString,
						Data: []dynamodb.AttributeData{"STRING"},
					},
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		Segment:       1,
		Select:        dynamodb.SelectCount,
		TableName:     "QUERY_TABLE",
		TotalSegments: 100,
	}
	expectedQuery := dynamodb.ScanQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestUpdateItemQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "Key": {
    "UPDATE_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "TableName": "UPDATE_ITEM_TABLE"
}
`)
	q := dynamodb.UpdateItemQuery{
		Key: map[string]dynamodb.AttributeValue{
			"UPDATE_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		TableName: "UPDATE_ITEM_TABLE",
	}
	expectedQuery := dynamodb.UpdateItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestUpdateItemQuery_Full(t *testing.T) {
	expectedJSON := []byte(`
{
  "AttributeUpdates": {
    "ATTR1": {
      "Action": "PUT",
      "Value": {
        "S": "STRING"
      }
    },
    "ATTR2": {
      "Action": "DELETE"
    }
  },
  "ConditionalOperator": "OR",
  "Expected": {
    "UPDATE_ITEM_QUERY_KEY2": {
      "Exists": false
    },
    "UPDATE_ITEM_QUERY_KEY": {
      "Value": {
          "SS": [
            "STRING1",
            "STRING2"
          ]
       },
      "Exists": true
    }
  },
  "Key": {
    "UPDATE_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD",
  "TableName": "UPDATE_ITEM_TABLE"
}
`)
	q := dynamodb.UpdateItemQuery{
		AttributeUpdates: map[string]dynamodb.AttributeUpdate{
			"ATTR1": dynamodb.AttributeUpdate{
				Action: dynamodb.ActionPut,
				Value: dynamodb.AttributeValue{
					Type: dynamodb.TypeString,
					Data: []dynamodb.AttributeData{"STRING"},
				},
			},
			"ATTR2": dynamodb.AttributeUpdate{
				Action: dynamodb.ActionDelete,
			},
		},
		ConditionalOperator: dynamodb.CondOpOr,
		Expected: map[string]dynamodb.DeprecatedCondition{
			"UPDATE_ITEM_QUERY_KEY": dynamodb.DeprecatedCondition{
				Value: dynamodb.AttributeValue{
					Type: dynamodb.TypeStringSet,
					Data: []dynamodb.AttributeData{"STRING1", "STRING2"},
				},
				Exists: true,
			},
			"UPDATE_ITEM_QUERY_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		Key: map[string]dynamodb.AttributeValue{
			"UPDATE_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "UPDATE_ITEM_TABLE",
	}
	expectedQuery := dynamodb.UpdateItemQuery{}
	if assert.NoError(t, json.Unmarshal(expectedJSON, &expectedQuery)) {
		assert.Equal(t, q, expectedQuery)
	}
}

func TestQueryBuilder(t *testing.T) {
	suite.Run(t, new(QueryBuilderSuite))
}
