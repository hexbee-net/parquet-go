package schema

import (
	"strings"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/datastore"
	"github.com/hexbee-net/parquet/parquet"
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
	GetSchemaDefinition() *SchemaDefinition
	SetSchemaDefinition(*SchemaDefinition) error

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
	schemaDef      *SchemaDefinition
	Root           *Column
	numRecords     int64
	readOnly       bool
	selectedColumn []string // selected columns in reading. Empty means all the columns.
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
			params: &datastore.ColumnParameters{
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

func (s *Schema) Columns() []*Column {
	var (
		ret []*Column
		fn  func([]*Column)
	)

	fn = func(columns []*Column) {
		for i := range columns {
			if columns[i].data != nil {
				ret = append(ret, columns[i])
			} else {
				fn(columns[i].children)
			}
		}
	}

	s.ensureRoot()
	fn(s.Root.children)

	return ret
}

func (s *Schema) GetColumnByName(path string) *Column {
	data := s.Columns()
	for i := range data {
		if data[i].flatName == path {
			return data[i]
		}
	}

	return nil
}

func (s *Schema) GetSchemaDefinition() *SchemaDefinition {
	//TODO: Check what's going on here
	def, err := ParseSchemaDefinition(s.schemaDef.String())
	if err != nil {
		panic(err)
	}

	return def
}

func (s *Schema) SetSchemaDefinition(schemaDefinition *SchemaDefinition) error {
	s.schemaDef = schemaDefinition

	root, err := s.schemaDef.RootColumn.CreateColumn()
	if err != nil {
		return err
	}

	s.Root = root

	for _, c := range s.Root.children {
		recursiveFix(c, "", 0, 0)
	}

	return nil
}

//TODO: rename to GetNumRecords.
func (s *Schema) RowGroupNumRecords() int64 {
	return s.numRecords
}

func (s *Schema) SetNumRecords(n int64) {
	s.numRecords = n
}

func (s *Schema) ResetData() {
	data := s.Columns()
	for i := range data {
		data[i].data.Reset(data[i].rep, data[i].maxR, data[i].maxD)
	}

	s.numRecords = 0
}

func (s *Schema) GetSchemaArray() []*parquet.SchemaElement {
	s.ensureRoot()

	elem := s.Root.GetSchemaArray()

	// the root doesn't have repetition type
	elem[0].RepetitionType = nil

	return elem
}

func (s *Schema) GetData() (map[string]interface{}, error) {
	d, _, err := s.Root.GetData()
	if err != nil {
		return nil, err
	}

	if d.(map[string]interface{}) == nil {
		d = make(map[string]interface{}) // just non nil root doc
	}

	return d.(map[string]interface{}), nil
}

func (s *Schema) SetSelectedColumns(selected ...string) {
	s.selectedColumn = selected
}

func (s *Schema) IsSelected(colPath string) bool {
	if len(s.selectedColumn) == 0 {
		return true
	}

	for _, pattern := range s.selectedColumn {
		if pattern == colPath {
			return true
		}

		if strings.HasPrefix(colPath, pattern+".") {
			return true
		}
	}

	return false
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

func recursiveFix(col *Column, path string, maxR, maxD uint16) {
	if col.rep != parquet.FieldRepetitionType_REQUIRED {
		maxD++
	}

	if col.rep == parquet.FieldRepetitionType_REPEATED {
		maxR++
	}

	col.maxR = maxR
	col.maxD = maxD
	col.flatName = path + "." + col.name

	if path == "" {
		col.flatName = col.name
	}

	if col.data != nil {
		col.data.Reset(col.rep, col.maxR, col.maxD)
		return
	}

	for i := range col.children {
		recursiveFix(col.children[i], col.flatName, maxR, maxD)
	}
}
