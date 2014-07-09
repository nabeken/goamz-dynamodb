package dynamodb

import (
	"log"

	"github.com/bitly/go-simplejson"
)

type Table struct {
	Server *Server
	Name   string
	Key    PrimaryKey
}

type describeTableResponse struct {
	Table TableDescription
}

func (t *Table) DescribeTable() (*TableDescription, error) {
	return t.Server.DescribeTable(t.Name)
}

func (t *Table) Query(attributeComparisons []AttributeComparison) ([]map[string]*Attribute, error) {
	return runQuery(t, QueryQuery{
		TableName:     t.Name,
		KeyConditions: buildConditions(attributeComparisons),
	})
}

func (t *Table) QueryOnIndex(attributeComparisons []AttributeComparison, indexName string) ([]map[string]*Attribute, error) {
	return runQuery(t, QueryQuery{
		TableName:     t.Name,
		KeyConditions: buildConditions(attributeComparisons),
		IndexName:     indexName,
	})
}

func (t *Table) LimitedQuery(attributeComparisons []AttributeComparison, limit int64) ([]map[string]*Attribute, error) {
	return runQuery(t, QueryQuery{
		TableName:     t.Name,
		KeyConditions: buildConditions(attributeComparisons),
		Limit:         uint(limit),
	})
}

func (t *Table) LimitedQueryOnIndex(attributeComparisons []AttributeComparison, indexName string, limit int64) ([]map[string]*Attribute, error) {
	return runQuery(t, QueryQuery{
		TableName:     t.Name,
		KeyConditions: buildConditions(attributeComparisons),
		IndexName:     indexName,
		Limit:         uint(limit),
	})
}

func (t *Table) CountQuery(attributeComparisons []AttributeComparison) (int64, error) {
	q := QueryQuery{
		TableName:     t.Name,
		KeyConditions: buildConditions(attributeComparisons),
		Select:        SelectCount,
	}
	jsonResponse, err := t.Server.queryServer(target("Query"), q)
	if err != nil {
		return 0, err
	}
	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return 0, err
	}

	itemCount, err := json.Get("Count").Int64()
	if err != nil {
		return 0, err
	}

	return itemCount, nil
}

func runQuery(t *Table, q interface{}) ([]map[string]*Attribute, error) {
	jsonResponse, err := t.Server.queryServer(target("Query"), q)
	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return nil, err
	}

	itemCount, err := json.Get("Count").Int()
	if err != nil {
		return nil, &UnexpectedResponseError{jsonResponse}
	}

	results := make([]map[string]*Attribute, itemCount)

	for i, _ := range results {
		item, err := json.Get("Items").GetIndex(i).Map()
		if err != nil {
			return nil, &UnexpectedResponseError{jsonResponse}
		}
		results[i] = parseAttributes(item)
	}
	return results, nil
}

func (t *Table) FetchPartialResults(query interface{}) ([]map[string]*Attribute, *Key, error) {
	jsonResponse, err := t.Server.queryServer(target("Scan"), query)
	if err != nil {
		return nil, nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return nil, nil, err
	}

	itemCount, err := json.Get("Count").Int()
	if err != nil {
		return nil, nil, &UnexpectedResponseError{jsonResponse}
	}

	results := make([]map[string]*Attribute, itemCount)
	for i, _ := range results {
		item, err := json.Get("Items").GetIndex(i).Map()
		if err != nil {
			return nil, nil, &UnexpectedResponseError{jsonResponse}
		}
		results[i] = parseAttributes(item)
	}

	var lastEvaluatedKey *Key
	if lastKeyMap := json.Get("LastEvaluatedKey").MustMap(); lastKeyMap != nil {
		lastEvaluatedKey = parseKey(t, lastKeyMap)
	}

	return results, lastEvaluatedKey, nil
}

func (t *Table) ScanPartial(attributeComparisons []AttributeComparison, exclusiveStartKey *Key) ([]map[string]*Attribute, *Key, error) {
	return t.ParallelScanPartialLimit(attributeComparisons, exclusiveStartKey, 0, 0, 0)
}

func (t *Table) ScanPartialLimit(attributeComparisons []AttributeComparison, exclusiveStartKey *Key, limit int64) ([]map[string]*Attribute, *Key, error) {
	return t.ParallelScanPartialLimit(attributeComparisons, exclusiveStartKey, 0, 0, limit)
}

func (t *Table) ParallelScanPartial(attributeComparisons []AttributeComparison, exclusiveStartKey *Key, segment, totalSegments int) ([]map[string]*Attribute, *Key, error) {
	return t.ParallelScanPartialLimit(attributeComparisons, exclusiveStartKey, segment, totalSegments, 0)
}

func (t *Table) ParallelScanPartialLimit(attributeComparisons []AttributeComparison, exclusiveStartKey *Key, segment, totalSegments int, limit int64) ([]map[string]*Attribute, *Key, error) {
	q := ScanQuery{
		ScanFilter: buildConditions(attributeComparisons),
		TableName:  t.Name,
	}

	if exclusiveStartKey != nil {
		q.ExclusiveStartKey = buildAttributeValueFromKey(t, exclusiveStartKey)
	}
	if totalSegments > 0 {
		q.TotalSegments = uint(totalSegments)
		q.Segment = uint(segment)
	}
	if limit > 0 {
		q.Limit = uint(limit)
	}
	return t.FetchPartialResults(q)
}

func (t *Table) FetchResults(query interface{}) ([]map[string]*Attribute, error) {
	results, _, err := t.FetchPartialResults(query)
	return results, err
}

func (t *Table) Scan(attributeComparisons []AttributeComparison) ([]map[string]*Attribute, error) {
	return t.FetchResults(ScanQuery{
		ScanFilter: buildConditions(attributeComparisons),
		TableName:  t.Name,
	})
}

func (t *Table) ParallelScan(attributeComparisons []AttributeComparison, segment int, totalSegments int) ([]map[string]*Attribute, error) {
	return t.FetchResults(ScanQuery{
		ScanFilter:    buildConditions(attributeComparisons),
		Segment:       uint(segment),
		TableName:     t.Name,
		TotalSegments: uint(totalSegments),
	})
}

func (t *Table) BatchGetItems(keys []Key) *BatchGetItem {
	batchGetItem := &BatchGetItem{t.Server, make(map[*Table][]Key)}

	batchGetItem.Keys[t] = keys
	return batchGetItem
}

func (t *Table) GetItem(key *Key) (map[string]*Attribute, error) {
	return t.getItem(key, false)
}

func (t *Table) GetItemConsistent(key *Key, consistentRead bool) (map[string]*Attribute, error) {
	return t.getItem(key, consistentRead)
}

func (t *Table) getItem(key *Key, consistentRead bool) (map[string]*Attribute, error) {
	q := GetItemQuery{
		ConsistentRead: consistentRead,
		Key:            buildAttributeValueFromKey(t, key),
		TableName:      t.Name,
	}

	jsonResponse, err := t.Server.queryServer(target("GetItem"), q)
	if err != nil {
		return nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return nil, err
	}

	itemJson, ok := json.CheckGet("Item")
	if !ok {
		// We got an empty from amz. The item doesn't exist.
		return nil, ErrNotFound
	}

	item, err := itemJson.Map()
	if err != nil {
		return nil, &UnexpectedResponseError{jsonResponse}
	}

	return parseAttributes(item), nil

}

func (t *Table) PutItem(hashKey string, rangeKey string, attributes []Attribute) (bool, error) {
	return t.putItem(hashKey, rangeKey, attributes, nil)
}

func (t *Table) ConditionalPutItem(hashKey, rangeKey string, attributes, expected []Attribute) (bool, error) {
	return t.putItem(hashKey, rangeKey, attributes, expected)
}

func (t *Table) putItem(hashKey, rangeKey string, attributes, expected []Attribute) (bool, error) {
	if len(attributes) == 0 {
		return false, ErrAtLeastOneAttributeRequired
	}

	keys := t.Key.Clone(hashKey, rangeKey)
	attributes = append(attributes, keys...)

	q := PutItemQuery{
		Item:      buildAttributeValueFromAttributes(attributes),
		TableName: t.Name,
	}

	if expected != nil {
		q.Expected = buildDeprecatedConditions(expected)
	}

	jsonResponse, err := t.Server.queryServer(target("PutItem"), q)

	if err != nil {
		return false, err
	}

	_, err = simplejson.NewJson(jsonResponse)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *Table) deleteItem(key *Key, expected []Attribute) (bool, error) {
	q := DeleteItemQuery{
		Key:       buildAttributeValueFromKey(t, key),
		TableName: t.Name,
	}

	if expected != nil {
		q.Expected = buildDeprecatedConditions(expected)
	}

	jsonResponse, err := t.Server.queryServer(target("DeleteItem"), q)

	if err != nil {
		return false, err
	}

	_, err = simplejson.NewJson(jsonResponse)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *Table) DeleteItem(key *Key) (bool, error) {
	return t.deleteItem(key, nil)
}

func (t *Table) ConditionalDeleteItem(key *Key, expected []Attribute) (bool, error) {
	return t.deleteItem(key, expected)
}

func (t *Table) AddAttributes(key *Key, attributes []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, nil, "ADD")
}

func (t *Table) UpdateAttributes(key *Key, attributes []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, nil, "PUT")
}

func (t *Table) DeleteAttributes(key *Key, attributes []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, nil, "DELETE")
}

func (t *Table) ConditionalAddAttributes(key *Key, attributes, expected []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, expected, "ADD")
}

func (t *Table) ConditionalUpdateAttributes(key *Key, attributes, expected []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, expected, "PUT")
}

func (t *Table) ConditionalDeleteAttributes(key *Key, attributes, expected []Attribute) (bool, error) {
	return t.modifyAttributes(key, attributes, expected, "DELETE")
}

func (t *Table) modifyAttributes(key *Key, attributes, expected []Attribute, action string) (bool, error) {

	if len(attributes) == 0 {
		return false, ErrAtLeastOneAttributeRequired
	}

	q := UpdateItemQuery{
		Key:       buildAttributeValueFromKey(t, key),
		TableName: t.Name,
	}

	q.AttributeUpdates = buildAttributeUpdates(action, attributes)

	if expected != nil {
		q.Expected = buildDeprecatedConditions(expected)
	}

	jsonResponse, err := t.Server.queryServer(target("UpdateItem"), q)

	if err != nil {
		return false, err
	}

	_, err = simplejson.NewJson(jsonResponse)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *Table) BatchWriteItems(itemActions map[string][][]Attribute) *BatchWriteItem {
	batchWriteItem := &BatchWriteItem{t.Server, make(map[*Table]map[string][][]Attribute)}

	batchWriteItem.ItemActions[t] = itemActions
	return batchWriteItem
}

func parseAttributes(s map[string]interface{}) map[string]*Attribute {
	results := map[string]*Attribute{}

	for key, value := range s {
		if v, ok := value.(map[string]interface{}); ok {
			if val, ok := v[TYPE_STRING].(string); ok {
				results[key] = &Attribute{
					Type:  TYPE_STRING,
					Name:  key,
					Value: val,
				}
			} else if val, ok := v[TYPE_NUMBER].(string); ok {
				results[key] = &Attribute{
					Type:  TYPE_NUMBER,
					Name:  key,
					Value: val,
				}
			} else if val, ok := v[TYPE_BINARY].(string); ok {
				results[key] = &Attribute{
					Type:  TYPE_BINARY,
					Name:  key,
					Value: val,
				}
			} else if vals, ok := v[TYPE_STRING_SET].([]interface{}); ok {
				arry := make([]string, len(vals))
				for i, ivalue := range vals {
					if val, ok := ivalue.(string); ok {
						arry[i] = val
					}
				}
				results[key] = &Attribute{
					Type:      TYPE_STRING_SET,
					Name:      key,
					SetValues: arry,
				}
			} else if vals, ok := v[TYPE_NUMBER_SET].([]interface{}); ok {
				arry := make([]string, len(vals))
				for i, ivalue := range vals {
					if val, ok := ivalue.(string); ok {
						arry[i] = val
					}
				}
				results[key] = &Attribute{
					Type:      TYPE_NUMBER_SET,
					Name:      key,
					SetValues: arry,
				}
			} else if vals, ok := v[TYPE_BINARY_SET].([]interface{}); ok {
				arry := make([]string, len(vals))
				for i, ivalue := range vals {
					if val, ok := ivalue.(string); ok {
						arry[i] = val
					}
				}
				results[key] = &Attribute{
					Type:      TYPE_BINARY_SET,
					Name:      key,
					SetValues: arry,
				}
			}
		} else {
			log.Printf("dynamod: type assertion to map[string] interface{} failed '%s'", value)
		}

	}

	return results
}

func parseKey(t *Table, s map[string]interface{}) *Key {
	k := &Key{}

	hk := t.Key.KeyAttribute
	if v, ok := s[hk.Name].(map[string]interface{}); ok {
		switch hk.Type {
		case TYPE_NUMBER, TYPE_STRING, TYPE_BINARY:
			if key, ok := v[hk.Type].(string); ok {
				k.HashKey = key
			} else {
				// log.Printf("type assertion to string failed for : %s\n", hk.Type)
				return nil
			}
		default:
			// log.Printf("invalid primary key hash type : %s\n", hk.Type)
			return nil
		}
	} else {
		// log.Printf("type assertion to map[string]interface{} failed for : %s\n", hk.Name)
		return nil
	}

	if t.Key.HasRange() {
		rk := t.Key.RangeAttribute
		if v, ok := s[rk.Name].(map[string]interface{}); ok {
			switch rk.Type {
			case TYPE_NUMBER, TYPE_STRING, TYPE_BINARY:
				if key, ok := v[rk.Type].(string); ok {
					k.RangeKey = key
				} else {
					// log.Printf("type assertion to string failed for : %s\n", rk.Type)
					return nil
				}
			default:
				// log.Printf("invalid primary key range type : %s\n", rk.Type)
				return nil
			}
		} else {
			// log.Printf("type assertion to map[string]interface{} failed for : %s\n", rk.Name)
			return nil
		}
	}

	return k
}
