package schema

// SchemaDefinition represents a valid textual schema definition.
type SchemaDefinition struct {
	RootColumn *ColumnDefinition
}

func ParseSchemaDefinition(schemaText string) (*SchemaDefinition, error) {
	panic("implement me")
}

// String returns a textual representation of the schema definition. This textual representation
// adheres to the format accepted by the ParseSchemaDefinition function. A textual schema definition
// parsed by ParseSchemaDefinition and turned back into a string by this method repeatedly will
// always remain the same, save for differences in the emitted whitespaces.
func (d *SchemaDefinition) String() string {
	panic("implement me")
}
