package dynamodb

type AttributeDefinition struct {
	Name string `json:"AttributeName"`
	Type string `json:"AttributeType"`
}

func (a *AttributeDefinition) GetEmptyAttribute() *Attribute {
	switch a.Type {
	case "S":
		return NewStringAttribute(a.Name, "")
	case "N":
		return NewNumericAttribute(a.Name, "")
	case "B":
		return NewBinaryAttribute(a.Name, "")
	default:
		return nil
	}
}

func findAttributeDefinitionByName(ads []AttributeDefinition, name string) *AttributeDefinition {
	for _, a := range ads {
		if a.Name == name {
			return &a
		}
	}
	return nil
}

type KeySchema struct {
	AttributeName string
	KeyType       string
}

type Projection struct {
	ProjectionType   string
	NonKeyAttributes []string
}

type GlobalSecondaryIndex struct {
	IndexName             string
	IndexSizeBytes        int64
	ItemCount             int64
	KeySchema             []KeySchema
	Projection            Projection
	ProvisionedThroughput ProvisionedThroughput
}

type LocalSecondaryIndex struct {
	IndexName      string
	IndexSizeBytes int64
	ItemCount      int64
	KeySchema      []KeySchema
	Projection     Projection
}

type ProvisionedThroughput struct {
	NumberOfDecreasesToday int64
	ReadCapacityUnits      int64
	WriteCapacityUnits     int64
}

type TableDescription struct {
	AttributeDefinitions   []AttributeDefinition
	CreationDateTime       float64
	ItemCount              int64
	KeySchema              []KeySchema
	LocalSecondaryIndexes  []LocalSecondaryIndex
	GlobalSecondaryIndexes []GlobalSecondaryIndex
	ProvisionedThroughput  ProvisionedThroughput
	TableName              string
	TableSizeBytes         int64
	TableStatus            string
}

func (t *TableDescription) BuildPrimaryKey() (pk PrimaryKey, err error) {
	for _, k := range t.KeySchema {
		var attr *Attribute
		ad := findAttributeDefinitionByName(t.AttributeDefinitions, k.AttributeName)
		if ad == nil {
			return pk, ErrInconsistencyInTableDescription
		}
		attr = ad.GetEmptyAttribute()
		if attr == nil {
			return pk, ErrInconsistencyInTableDescription
		}

		switch k.KeyType {
		case "HASH":
			pk.KeyAttribute = attr
		case "RANGE":
			pk.RangeAttribute = attr
		}
	}
	return
}
