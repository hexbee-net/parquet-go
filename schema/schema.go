package schema

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema/definition"
)

// schemaCommon contains methods shared by FileReader and FileWriter
// to retrieve and set information related to the parquet schema and
// columns that are used by the reader resp. writer.
type schemaCommon interface {
	// Columns return only data columns, not all columns
	Columns() []*Column

	// Return a column by its name
	GetColumnByName(path string) *Column

	// GetSchemaDefinition returns the schema definition.
	GetSchemaDefinition() *definition.Schema
	SetSchemaDefinition(*definition.Schema) error

	// Internal functions
	RowGroupNumRecords() int64
	ResetData()
	GetSchemaArray() []*parquet.SchemaElement
}

// Reader is an interface with methods necessary in the FileReader.
type Reader interface {
	schemaCommon

	SetNumRecords(int64)
	GetData() (map[string]interface{}, error)
	SetSelectedColumns(selected ...string)
	IsSelected(string) bool
}

// Writer is an interface with methods necessary in the FileWriter
// to add groups and columns and to write data.
type Writer interface {
	schemaCommon

	AddData(m map[string]interface{}) error
	AddGroup(path string, rep parquet.FieldRepetitionType) error
	AddColumn(path string, col *Column) error
	DataSize() int64
}


type Schema struct {
	schemaDef      *definition.Schema
	Root           *Column
	numRecords     int64
	readOnly       bool
	selectedColumn []string // selected columns in reading. Empty means all the columns.
}

func (s *Schema) Columns() []*Column {
	panic("implement me")
}

func (s *Schema) GetColumnByName(path string) *Column {
	panic("implement me")
}

func (s *Schema) GetSchemaDefinition() *definition.Schema {
	panic("implement me")
}

func (s *Schema) SetSchemaDefinition(schema *definition.Schema) error {
	panic("implement me")
}

func (s *Schema) RowGroupNumRecords() int64 {
	panic("implement me")
}

func (s *Schema) ResetData() {
	panic("implement me")
}

func (s *Schema) GetSchemaArray() []*parquet.SchemaElement {
	panic("implement me")
}

func (s *Schema) SetNumRecords(i int64) {
	panic("implement me")
}

func (s *Schema) GetData() (map[string]interface{}, error) {
	panic("implement me")
}

func (s *Schema) SetSelectedColumns(selected ...string) {
	panic("implement me")
}

func (s *Schema) IsSelected(col string) bool {
	panic("implement me")
}

func LoadSchema(schema []*parquet.SchemaElement) (s *Schema, err error) {
	root := schema[0]
	schema = schema[1:]

	s = &Schema{
		readOnly: true,
		Root: &Column{
			index:     0,
			name:      root.Name,
			flatName:  "",
			nameArray: nil,
			data:      nil,
			children:  make([]*Column, 0, len(schema)),
			rep:       0,
			maxR:      0,
			maxD:      0,
			parent:    0,
			element:   root,
			params: &ColumnParameters{
				LogicalType:   root.LogicalType,
				ConvertedType: root.ConvertedType,
				TypeLength:    root.TypeLength,
				FieldID:       root.FieldID,
			},
		},
	}

	for idx := 0; idx < len(schema); {
		c := &Column{}

		if schema[idx].Type == nil {
			idx, err = c.readGroupSchema(schema, "", idx, 0, 0)
		} else {
			idx, err = c.readColumnSchema(schema, "", idx, 0, 0)
		}

		if err != nil {
			return nil, errors.WithStack(err)
		}

		s.Root.children = append(s.Root.children, c)
	}

	s.sortIndex()

	s.schemaDef = s.Root.AsColumnDefinition().AsSchemaDefinition()

	return s, nil
}

func (s *Schema) sortIndex() {
	var fn func(c *[]*Column)

	idx := 0
	fn = func(c *[]*Column) {
		if c == nil {
			return
		}

		for data := range *c {
			if (*c)[data].data != nil {
				(*c)[data].index = idx
				idx++
			} else {
				fn(&(*c)[data].children)
			}
		}
	}

	s.ensureRoot()

	fn(&s.Root.children)
}

func (s *Schema) ensureRoot() {
	if s.Root == nil {
		s.Root = &Column{
			index:    0,
			name:     "msg",
			flatName: "", // the flat name for root element is empty
			data:     nil,
			children: []*Column{},
			rep:      0,
			maxR:     0,
			maxD:     0,
			element:  nil,
		}
	}
}
