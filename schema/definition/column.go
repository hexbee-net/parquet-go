package definition

import "github.com/hexbee-net/parquet/parquet"

// Column represents the schema definition of a column and optionally its children.
type Column struct {
	SchemaElement *parquet.SchemaElement
	Children      []*Column
}

// AsSchemaDefinition creates a new schema definition from the provided column definition.
func (c *Column) AsSchemaDefinition() *Schema {
	return &Schema{
		RootColumn: c,
	}
}
