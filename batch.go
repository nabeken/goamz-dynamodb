package dynamodb

import (
	"errors"

	"github.com/bitly/go-simplejson"
)

type BatchGetItem struct {
	Server *Server
	Keys   map[*Table][]Key
}

type BatchWriteItem struct {
	Server      *Server
	ItemActions map[*Table]map[string][][]Attribute
}

func (batchGetItem *BatchGetItem) AddTable(t *Table, keys *[]Key) *BatchGetItem {
	batchGetItem.Keys[t] = *keys
	return batchGetItem
}

func (batchWriteItem *BatchWriteItem) AddTable(t *Table, itemActions *map[string][][]Attribute) *BatchWriteItem {
	batchWriteItem.ItemActions[t] = *itemActions
	return batchWriteItem
}

func (batchGetItem *BatchGetItem) Execute() (map[string][]map[string]*Attribute, error) {
	q := NewEmptyQuery()
	q.AddGetRequestItems(batchGetItem.Keys)

	jsonResponse, err := batchGetItem.Server.queryServer(target("BatchGetItem"), q)
	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	results := make(map[string][]map[string]*Attribute)
	tables, err := json.Get("Responses").Map()
	if err != nil {
		return nil, &UnexpectedResponseError{jsonResponse}
	}

	for table, entries := range tables {
		var tableResult []map[string]*Attribute

		jsonEntriesArray, ok := entries.([]interface{})
		if !ok {
			return nil, &UnexpectedResponseError{jsonResponse}
		}

		for _, entry := range jsonEntriesArray {
			item, ok := entry.(map[string]interface{})
			if !ok {
				return nil, &UnexpectedResponseError{jsonResponse}
			}

			unmarshalledItem := parseAttributes(item)
			tableResult = append(tableResult, unmarshalledItem)
		}

		results[table] = tableResult
	}

	return results, nil
}

func (batchWriteItem *BatchWriteItem) Execute() (map[string]interface{}, error) {
	q := NewEmptyQuery()
	q.AddWriteRequestItems(batchWriteItem.ItemActions)

	jsonResponse, err := batchWriteItem.Server.queryServer(target("BatchWriteItem"), q)

	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)

	if err != nil {
		return nil, err
	}

	unprocessed, err := json.Get("UnprocessedItems").Map()
	if err != nil {
		return nil, &UnexpectedResponseError{jsonResponse}
	}

	if len(unprocessed) == 0 {
		return nil, nil
	} else {
		return unprocessed, errors.New("One or more unprocessed items.")
	}

}
