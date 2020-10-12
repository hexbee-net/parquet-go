package schema

import (
	"github.com/hexbee-net/parquet/datastore"
	"github.com/hexbee-net/parquet/parquet"
)

// ColumnDefinition represents the schema definition of a column and optionally its children.
type ColumnDefinition struct {
	SchemaElement *parquet.SchemaElement
	Children      []*ColumnDefinition
}

// AsSchemaDefinition creates a new schema definition from the provided column definition.
func (c *ColumnDefinition) AsSchemaDefinition() *SchemaDefinition {
	return &SchemaDefinition{
		RootColumn: c,
	}
}

func (c *ColumnDefinition) CreateColumn() (*Column, error) {
	params := &ColumnParameters{
		LogicalType:   c.SchemaElement.LogicalType,
		ConvertedType: c.SchemaElement.ConvertedType,
		TypeLength:    c.SchemaElement.TypeLength,
		FieldID:       c.SchemaElement.FieldID,
		Scale:         c.SchemaElement.Scale,
		Precision:     c.SchemaElement.Precision,
	}

	col := &Column{
		name:   c.SchemaElement.GetName(),
		rep:    c.SchemaElement.GetRepetitionType(),
		params: params,
	}

	if len(c.Children) > 0 {
		for _, c := range c.Children {
			childColumn, err := c.CreateColumn()
			if err != nil {
				return nil, err
			}
			col.children = append(col.children, childColumn)
		}
	} else {
		dataColumn, err := datastore.GetColumnStore(c.SchemaElement, params)
		if err != nil {
			return nil, err
		}
		col.data = dataColumn
	}

	col.element = col.buildElement()

	return col, nil}
