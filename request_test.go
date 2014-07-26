package dynamodb_test

import (
	"encoding/json"
	"testing"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestCreateTable_Least(t *testing.T) {
	expectedJSON := []byte(`
{
	"AttributeDefinitions": [
		{
			"AttributeName": "HASHKEY",
			"AttributeType": "S"
		},
		{
			"AttributeName": "RANGEKEY",
			"AttributeType": "N"
		}
	],
	"KeySchema": [
		{
			"AttributeName": "HASHKEY",
			"KeyType": "HASH"
		},
		{
			"AttributeName": "RANGEKEY",
			"KeyType": "RANGE"
		}
	],
	"ProvisionedThroughput": {
		"ReadCapacityUnits": 10,
		"WriteCapacityUnits": 10
	},
	"TableName": "CREATE_TABLE_REQUEST"
}
`)
	q := dynamodb.CreateTable{
		Name: "CREATE_TABLE_REQUEST",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"HASHKEY", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"RANGEKEY", dynamodb.TypeNumber},
		},
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"HASHKEY", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"RANGEKEY", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	expectedRequest := dynamodb.CreateTable{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestCreateTable_Full(t *testing.T) {
	expectedJSON := []byte(`
{
	"GlobalSecondaryIndexes": [
		{
			"IndexName": "GSI1",
			"KeySchema": [
				{
					"AttributeName": "ATTR1",
					"KeyType": "HASH"
				},
				{
					"AttributeName": "ATTR2",
					"KeyType": "RANGE"
				}
			],
			"Projection": {
				"ProjectionType": "INCLUDE",
				"NonKeyAttributes": [
					"ATTR"
				]
			},
			"ProvisionedThroughput": {
				"ReadCapacityUnits": 10,
				"WriteCapacityUnits": 10
			}
		},
		{
			"IndexName": "GSI2",
			"KeySchema": [
				{
					"AttributeName": "ATTR2",
					"KeyType": "HASH"
				},
				{
					"AttributeName": "ATTR1",
					"KeyType": "RANGE"
				}
			],
			"Projection": {
				"ProjectionType": "INCLUDE",
				"NonKeyAttributes": [
					"ATTR"
				]
			},
			"ProvisionedThroughput": {
				"ReadCapacityUnits": 10,
				"WriteCapacityUnits": 10
			}
		}
	],
	"LocalSecondaryIndexes": [
		{
			"IndexName": "LSI1",
			"KeySchema": [
				{
					"AttributeName": "HASHKEY",
					"KeyType": "HASH"
				},
				{
					"AttributeName": "ATTR1",
					"KeyType": "RANGE"
				}
			],
			"Projection": {
				"ProjectionType": "INCLUDE",
				"NonKeyAttributes": [
					"ATTR"
				]
			}
		},
		{
			"IndexName": "LSI2",
			"KeySchema": [
				{
					"AttributeName": "HASHKEY",
					"KeyType": "HASH"
				},
				{
					"AttributeName": "ATTR2",
					"KeyType": "RANGE"
				}
			],
			"Projection": {
				"ProjectionType": "INCLUDE",
				"NonKeyAttributes": [
					"ATTR"
				]
			}
		}
	]
}
`)
	q := dynamodb.TableOption{
		GlobalSecondaryIndexes: []dynamodb.GlobalSecondaryIndex{
			dynamodb.GlobalSecondaryIndex{
				IndexName: "GSI1",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR1",
						KeyType:       dynamodb.KeyTypeHash,
					},
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR2",
						KeyType:       dynamodb.KeyTypeRange,
					},
				},
				Projection: dynamodb.Projection{
					ProjectionType:   dynamodb.ProjectionTypeInclude,
					NonKeyAttributes: []string{"ATTR"},
				},
				ProvisionedThroughput: dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  10,
					WriteCapacityUnits: 10,
				},
			},
			dynamodb.GlobalSecondaryIndex{
				IndexName: "GSI2",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR2",
						KeyType:       dynamodb.KeyTypeHash,
					},
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR1",
						KeyType:       dynamodb.KeyTypeRange,
					},
				},
				Projection: dynamodb.Projection{
					ProjectionType:   dynamodb.ProjectionTypeInclude,
					NonKeyAttributes: []string{"ATTR"},
				},
				ProvisionedThroughput: dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  10,
					WriteCapacityUnits: 10,
				},
			},
		},
		LocalSecondaryIndexes: []dynamodb.LocalSecondaryIndex{
			dynamodb.LocalSecondaryIndex{
				IndexName: "LSI1",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{
						AttributeName: "HASHKEY",
						KeyType:       dynamodb.KeyTypeHash,
					},
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR1",
						KeyType:       dynamodb.KeyTypeRange,
					},
				},
				Projection: dynamodb.Projection{
					ProjectionType:   dynamodb.ProjectionTypeInclude,
					NonKeyAttributes: []string{"ATTR"},
				},
			},
			dynamodb.LocalSecondaryIndex{
				IndexName: "LSI2",
				KeySchema: []dynamodb.KeySchemaElement{
					dynamodb.KeySchemaElement{
						AttributeName: "HASHKEY",
						KeyType:       dynamodb.KeyTypeHash,
					},
					dynamodb.KeySchemaElement{
						AttributeName: "ATTR2",
						KeyType:       dynamodb.KeyTypeRange,
					},
				},
				Projection: dynamodb.Projection{
					ProjectionType:   dynamodb.ProjectionTypeInclude,
					NonKeyAttributes: []string{"ATTR"},
				},
			},
		},
	}
	expectedRequest := dynamodb.TableOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestDeleteItemOption(t *testing.T) {
	expectedJSON := []byte(`
{
  "ConditionalOperator": "OR",
  "Expected": {
    "DELETE_ITEM_REQUEST_KEY": {
      "Value": {
          "S": "STRING"
      },
      "Exists": true
    },
    "DELETE_ITEM_REQUEST_KEY2": {
      "Exists": false
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD"
}
`)
	q := dynamodb.DeleteItemOption{
		ConditionalOperator: dynamodb.CondOpOr,
		Expected: map[string]dynamodb.DeprecatedCondition{
			"DELETE_ITEM_REQUEST_KEY": dynamodb.DeprecatedCondition{
				Value:  dynamodb.NewString("STRING"),
				Exists: true,
			},
			"DELETE_ITEM_REQUEST_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
	}
	expectedRequest := dynamodb.DeleteItemOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestGetItemOption(t *testing.T) {
	expectedJSON := []byte(`
{
  "AttributesToGet": [
    "ATTR1",
    "ATTR2"
  ],
  "ConsistentRead": true,
  "ReturnConsumedCapacity": "TOTAL"
}
`)
	q := dynamodb.GetItemOption{
		AttributesToGet:        []string{"ATTR1", "ATTR2"},
		ConsistentRead:         true,
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
	}
	expectedRequest := dynamodb.GetItemOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestListTableOption(t *testing.T) {
	expectedJSON := []byte(`
{
	"ExclusiveStartTableName": "LIST_TABLES_REQUEST_TABLE",
	"Limit": 10
}
`)
	q := dynamodb.ListTablesOption{
		ExclusiveStartTableName: "LIST_TABLES_REQUEST_TABLE",
		Limit: 10,
	}
	expectedRequest := dynamodb.ListTablesOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestPutItemOption(t *testing.T) {
	expectedJSON := []byte(`
{
  "ConditionalOperator": "OR",
  "Expected": {
    "PUT_ITEM_REQUEST_KEY": {
      "Value": {
          "S": "STRING"
      },
      "Exists": true
    },
    "PUT_ITEM_REQUEST_KEY2": {
      "Exists": false
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD"
}
`)
	q := dynamodb.PutItemOption{
		ConditionalOperator: dynamodb.CondOpOr,
		Expected: map[string]dynamodb.DeprecatedCondition{
			"PUT_ITEM_REQUEST_KEY": dynamodb.DeprecatedCondition{
				Value:  dynamodb.NewString("STRING"),
				Exists: true,
			},
			"PUT_ITEM_REQUEST_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
	}
	expectedRequest := dynamodb.PutItemOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestQueryOption(t *testing.T) {
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
  "Limit": 100,
  "QueryFilter": {
    "REQUEST_KEY": {
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
  "Select": "COUNT"
}
`)
	q := dynamodb.QueryOption{
		AttributesToGet:     []string{"ATTR1", "ATTR2"},
		ConditionalOperator: dynamodb.CondOpAnd,
		ConsistentRead:      true,
		ExclusiveStartKey: map[string]dynamodb.AttributeValue{
			"START": dynamodb.NewNumber(123456789),
		},
		IndexName: "MYGSI",
		Limit:     100,
		QueryFilter: map[string]dynamodb.Condition{
			"REQUEST_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.NewString("STRING"),
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
		ScanIndexForward:       true,
		Select:                 dynamodb.SelectCount,
	}
	expectedRequest := dynamodb.QueryOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestScanOption(t *testing.T) {
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
    "REQUEST_KEY": {
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
  "TotalSegments": 100
}
`)
	q := dynamodb.ScanOption{
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
			"REQUEST_KEY": dynamodb.Condition{
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
		TotalSegments: 100,
		Select:        dynamodb.SelectCount,
	}
	expectedRequest := dynamodb.ScanOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateItemOption(t *testing.T) {
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
    "UPDATE_ITEM_REQUEST_KEY2": {
      "Exists": false
    },
    "UPDATE_ITEM_REQUEST_KEY": {
      "Value": {
          "SS": [
            "STRING1",
            "STRING2"
          ]
       },
      "Exists": true
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD"
}
`)
	q := dynamodb.UpdateItemOption{
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
			"UPDATE_ITEM_REQUEST_KEY": dynamodb.DeprecatedCondition{
				Value: dynamodb.AttributeValue{
					Type: dynamodb.TypeStringSet,
					Data: []dynamodb.AttributeData{"STRING1", "STRING2"},
				},
				Exists: true,
			},
			"UPDATE_ITEM_REQUEST_KEY2": dynamodb.DeprecatedCondition{
				Exists: false,
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
	}
	expectedRequest := dynamodb.UpdateItemOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateTableOption(t *testing.T) {
	expectedJSON := []byte(`
{
	"GlobalSecondaryIndexUpdates": [
		{
			"Update": {
				"IndexName": "GSI1",
				"ProvisionedThroughput": {
					"ReadCapacityUnits": 10,
					"WriteCapacityUnits": 10
				}
			}
		},
		{
			"Update": {
				"IndexName": "GSI2",
				"ProvisionedThroughput": {
					"ReadCapacityUnits": 10,
					"WriteCapacityUnits": 10
				}
			}
		}
	],
	"ProvisionedThroughput": {
		"ReadCapacityUnits": 10,
		"WriteCapacityUnits": 10
	}
}
`)
	q := dynamodb.UpdateTableOption{
		GlobalSecondaryIndexUpdates: []dynamodb.GlobalSecondaryIndexUpdate{
			dynamodb.GlobalSecondaryIndexUpdate{
				Update: dynamodb.GlobalSecondaryIndexAction{
					IndexName: "GSI1",
					ProvisionedThroughput: dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  10,
						WriteCapacityUnits: 10,
					},
				},
			},
			dynamodb.GlobalSecondaryIndexUpdate{
				Update: dynamodb.GlobalSecondaryIndexAction{
					IndexName: "GSI2",
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
	expectedRequest := dynamodb.UpdateTableOption{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}
