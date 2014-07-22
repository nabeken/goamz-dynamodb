package dynamodb_test

import (
	"encoding/json"
	"testing"

	"github.com/nabeken/goamz-dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestCreateTableRequest_Least(t *testing.T) {
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
	q := dynamodb.CreateTableRequest{
		TableName: "CREATE_TABLE_REQUEST",
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
	expectedRequest := dynamodb.CreateTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestCreateTableRequest_Full(t *testing.T) {
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
		},
		{
			"AttributeName": "ATTR",
			"AttributeType": "N"
		},
		{
			"AttributeName": "ATTR1",
			"AttributeType": "N"
		},
		{
			"AttributeName": "ATTR2",
			"AttributeType": "N"
		}
	],
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
	],
	"ProvisionedThroughput": {
		"ReadCapacityUnits": 10,
		"WriteCapacityUnits": 10
	},
	"TableName": "CREATE_TABLE_REQUEST_FULL"
}
`)
	q := dynamodb.CreateTableRequest{
		TableName: "CREATE_TABLE_REQUEST_FULL",
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			dynamodb.AttributeDefinition{"HASHKEY", dynamodb.TypeString},
			dynamodb.AttributeDefinition{"RANGEKEY", dynamodb.TypeNumber},
			dynamodb.AttributeDefinition{"ATTR", dynamodb.TypeNumber},
			dynamodb.AttributeDefinition{"ATTR1", dynamodb.TypeNumber},
			dynamodb.AttributeDefinition{"ATTR2", dynamodb.TypeNumber},
		},
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
		KeySchema: []dynamodb.KeySchemaElement{
			dynamodb.KeySchemaElement{"HASHKEY", dynamodb.KeyTypeHash},
			dynamodb.KeySchemaElement{"RANGEKEY", dynamodb.KeyTypeRange},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  10,
			WriteCapacityUnits: 10,
		},
	}
	expectedRequest := dynamodb.CreateTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestDeleteItemRequest_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "Key": {
    "DELETE_ITEM_REQUEST_KEY": {
      "S": "STRING"
    }
  },
  "TableName": "DELETE_ITEM_REQUEST_TABLE"
}
`)
	q := dynamodb.DeleteItemRequest{
		Key: map[string]dynamodb.AttributeValue{
			"DELETE_ITEM_REQUEST_KEY": dynamodb.NewString("STRING"),
		},
		TableName: "DELETE_ITEM_REQUEST_TABLE",
	}
	expectedRequest := dynamodb.DeleteItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestDeleteItemRequest_Full(t *testing.T) {
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
  "Key": {
    "DELETE_ITEM_REQUEST_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD",
  "TableName": "DELETE_ITEM_REQUEST_TABLE"
}
`)
	q := dynamodb.DeleteItemRequest{
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
		Key: map[string]dynamodb.AttributeValue{
			"DELETE_ITEM_REQUEST_KEY": dynamodb.NewString("STRING"),
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "DELETE_ITEM_REQUEST_TABLE",
	}
	expectedRequest := dynamodb.DeleteItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestDeleteTableRequest(t *testing.T) {
	expectedJSON := []byte(`
{
  "TableName": "DELETE_TABLE_REQUEST_TABLE"
}
`)
	q := dynamodb.DeleteTableRequest{
		TableName: "DELETE_TABLE_REQUEST_TABLE",
	}
	expectedRequest := dynamodb.DeleteTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestDescribeTableRequest(t *testing.T) {
	expectedJSON := []byte(`
{
  "TableName": "DESCRIBE_TABLE_REQUEST_TABLE"
}
`)
	q := dynamodb.DescribeTableRequest{
		TableName: "DESCRIBE_TABLE_REQUEST_TABLE",
	}
	expectedRequest := dynamodb.DescribeTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestGetItemRequest_Least(t *testing.T) {
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
	q := dynamodb.GetItemRequest{
		Key: map[string]dynamodb.AttributeValue{
			"GET_ITEM_KEY": dynamodb.NewString("STRING"),
		},
		TableName: "GET_ITEM_TABLE",
	}
	expectedRequest := dynamodb.GetItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestGetItemRequest_Full(t *testing.T) {
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
	q := dynamodb.GetItemRequest{
		AttributesToGet: []string{"ATTR1", "ATTR2"},
		ConsistentRead:  true,
		Key: map[string]dynamodb.AttributeValue{
			"GET_ITEM_KEY": dynamodb.NewString("STRING"),
		},
		ReturnConsumedCapacity: dynamodb.ConsumedCapTotal,
		TableName:              "GET_ITEM_TABLE",
	}
	expectedRequest := dynamodb.GetItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestListTableRequest_Least(t *testing.T) {
	expectedJSON := []byte(`{}`)
	q := dynamodb.ListTablesRequest{}
	expectedRequest := dynamodb.ListTablesRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestListTableRequest_Full(t *testing.T) {
	expectedJSON := []byte(`
{
	"ExclusiveStartTableName": "LIST_TABLES_REQUEST_TABLE",
	"Limit": 10
}
`)
	q := dynamodb.ListTablesRequest{
		ExclusiveStartTableName: "LIST_TABLES_REQUEST_TABLE",
		Limit: 10,
	}
	expectedRequest := dynamodb.ListTablesRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestPutRequest_Least(t *testing.T) {
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
	q := dynamodb.PutItemRequest{
		Item: map[string]dynamodb.AttributeValue{
			"PUT_ITEM_KEY": dynamodb.NewString("STRING"),
		},
		TableName: "PUT_ITEM_TABLE",
	}
	expectedRequest := dynamodb.PutItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestPutRequest_Full(t *testing.T) {
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
  "Item": {
    "PUT_ITEM_KEY": {
      "S": "STRING"
    }
  },
  "ReturnConsumedCapacity": "TOTAL",
  "ReturnItemCollectionMetrics": "NONE",
  "ReturnValues": "ALL_OLD",
  "TableName": "PUT_ITEM_REQUEST_TABLE"
}
`)
	q := dynamodb.PutItemRequest{
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
		Item: map[string]dynamodb.AttributeValue{
			"PUT_ITEM_KEY": dynamodb.NewString("STRING"),
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "PUT_ITEM_REQUEST_TABLE",
	}
	expectedRequest := dynamodb.PutItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestQuery_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "KeyConditions": {
    "REQUEST_KEY": {
      "AttributeValueList": [
        {
          "S": "STRING"
        }
      ],
      "ComparisonOperator": "EQ"
    }
  },
  "TableName": "REQUEST_TABLE"
}
`)
	q := dynamodb.QueryRequest{
		KeyConditions: map[string]dynamodb.Condition{
			"REQUEST_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.NewString("STRING"),
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		TableName: "REQUEST_TABLE",
	}
	expectedRequest := dynamodb.QueryRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
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
    "REQUEST_KEY": {
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
  "Select": "COUNT",
  "TableName": "REQUEST_TABLE"
}
`)
	q := dynamodb.QueryRequest{
		AttributesToGet:     []string{"ATTR1", "ATTR2"},
		ConditionalOperator: dynamodb.CondOpAnd,
		ConsistentRead:      true,
		ExclusiveStartKey: map[string]dynamodb.AttributeValue{
			"START": dynamodb.NewNumber(123456789),
		},
		IndexName: "MYGSI",
		KeyConditions: map[string]dynamodb.Condition{
			"REQUEST_KEY": dynamodb.Condition{
				AttributeValueList: []dynamodb.AttributeValue{
					dynamodb.NewString("STRING"),
				},
				ComparisonOperator: dynamodb.CmpOpEQ,
			},
		},
		Limit: 100,
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
		TableName:              "REQUEST_TABLE",
	}
	expectedRequest := dynamodb.QueryRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestScan_Least(t *testing.T) {
	expectedJSON := []byte(`
{
  "TableName": "SCAN_TABLE"
}
`)
	q := dynamodb.ScanRequest{
		TableName: "SCAN_TABLE",
	}
	expectedRequest := dynamodb.ScanRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
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
  "TableName": "REQUEST_TABLE",
  "TotalSegments": 100
}
`)
	q := dynamodb.ScanRequest{
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
		Select:        dynamodb.SelectCount,
		TableName:     "REQUEST_TABLE",
		TotalSegments: 100,
	}
	expectedRequest := dynamodb.ScanRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateItemRequest_Least(t *testing.T) {
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
	q := dynamodb.UpdateItemRequest{
		Key: map[string]dynamodb.AttributeValue{
			"UPDATE_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		TableName: "UPDATE_ITEM_TABLE",
	}
	expectedRequest := dynamodb.UpdateItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateItemRequest_Full(t *testing.T) {
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
	q := dynamodb.UpdateItemRequest{
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
		Key: map[string]dynamodb.AttributeValue{
			"UPDATE_ITEM_KEY": dynamodb.AttributeValue{
				Type: dynamodb.TypeString,
				Data: []dynamodb.AttributeData{"STRING"},
			},
		},
		ReturnConsumedCapacity:      dynamodb.ConsumedCapTotal,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValuesAllOld,
		TableName:                   "UPDATE_ITEM_TABLE",
	}
	expectedRequest := dynamodb.UpdateItemRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateTableRequest_Least(t *testing.T) {
	expectedJSON := []byte(`
{
	"TableName": "UPDATE_TABLE_REQUEST_FULL"
}
`)
	q := dynamodb.UpdateTableRequest{
		TableName: "UPDATE_TABLE_REQUEST_FULL",
	}
	expectedRequest := dynamodb.UpdateTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}

func TestUpdateTableRequest_Full(t *testing.T) {
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
	},
	"TableName": "UPDATE_TABLE_REQUEST_FULL"
}
`)
	q := dynamodb.UpdateTableRequest{
		TableName: "UPDATE_TABLE_REQUEST_FULL",
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
	expectedRequest := dynamodb.UpdateTableRequest{}
	if !assert.NoError(t, json.Unmarshal(expectedJSON, &expectedRequest)) {
		t.Fail()
	}
	assert.Equal(t, q, expectedRequest)
}
