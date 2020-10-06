package datastore

import (
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
)

// parquetColumn is to convert a store to a parquet.SchemaElement.
type parquetColumn interface {
	ParquetType() parquet.Type
	repetitionType() parquet.FieldRepetitionType
	params() *ColumnParameters
}

type TypedColumnStore interface {
	parquetColumn

	reset(repetitionType parquet.FieldRepetitionType)

	// Min and Max in parquet byte
	maxValue() []byte
	minValue() []byte

	// Should extract the value, turn it into an array and check for min and max on all values in this
	getValues(v interface{}) ([]interface{}, error)
	sizeOf(v interface{}) int

	// the tricky append. this is a way of creating new "typed" array. the first interface is nil or an []T (T is the type,
	// not the interface) and value is from that type. the result should be always []T (array of that type)
	// exactly like the builtin append
	append(arrayIn interface{}, value interface{}) interface{}
}

// ColumnParameters contains common parameters related to a column.
type ColumnParameters struct {
	LogicalType   *parquet.LogicalType
	ConvertedType *parquet.ConvertedType
	TypeLength    *int32
	FieldID       *int32
	Scale         *int32
	Precision     *int32
}

// ColumnStore is the read/write implementation for a column.
// It buffers a single column's data that is to be written to a parquet file,
// knows how to encode this data and will choose an optimal way according to
// heuristics.
// It also ensures the correct decoding of column data to be read.
type ColumnStore struct {
	TypedColumnStore

	repTyp parquet.FieldRepetitionType

	Values *Dict

	DefinitionLevels *encoding.PackedArray
	RepetitionLevels *encoding.PackedArray

	enc     parquet.Encoding
	readPos int

	allowDict bool

	skipped bool
}
