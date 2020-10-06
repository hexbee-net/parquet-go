package schema

import "github.com/hexbee-net/parquet/parquet"

// Writer is an interface with methods necessary in the FileWriter
// to add groups and columns and to write data.
type Writer interface {
	schemaCommon

	AddData(m map[string]interface{}) error
	AddGroup(path string, rep parquet.FieldRepetitionType) error
	AddColumn(path string, col *Column) error
	DataSize() int64
}
