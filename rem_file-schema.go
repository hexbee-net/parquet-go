// +build removeme

package parquet

import (
	"strings"

	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
)

// SchemaCommon contains methods shared by FileReader and FileWriter
// to retrieve and set information related to the parquet schema and
// columns that are used by the reader resp. writer.
type SchemaCommon interface {
	// Columns return only data columns, not all columns
	Columns() []*Column

	// Return a column by its name
	GetColumnByName(path string) *Column

	// GetSchemaDefinition returns the schema definition.
	GetSchemaDefinition() *schema.Root
	SetSchemaDefinition(*schema.Root) error

	// Internal functions
	rowGroupNumRecords() int64
	resetData()
	getSchemaArray() []*parquet.SchemaElement
}

// SchemaReader is an interface with methods necessary in the FileReader.
type SchemaReader interface {
	SchemaCommon
	setNumRecords(int64)
	getData() (map[string]interface{}, error)
	setSelectedColumns(selected ...string)
	isSelected(string) bool
}

// SchemaWriter is an interface with methods necessary in the FileWriter
// to add groups and columns and to write data.
type SchemaWriter interface {
	SchemaCommon

	AddData(m map[string]interface{}) error
	AddGroup(path string, rep parquet.FieldRepetitionType) error
	AddColumn(path string, col *Column) error
	DataSize() int64
}

type FileSchema struct {
	schemaDef  *schema.Root
	root       *Column
	numRecords int64
	readOnly   int

	// selected columns in reading. if the size is zero, it means all the columns
	selectedColumn []string
}

func (s FileSchema) setSelectedColumns(selected ...string) {
	s.selectedColumn = selected
}

func (s FileSchema) isSelected(path string) bool {
	if len(s.selectedColumn) == 0 {
		return true
	}

	for _, pattern := range s.selectedColumn {
		if pattern == path {
			return true
		}

		if strings.HasPrefix(path, pattern+".") {
			return true
		}
	}

	return false
}
