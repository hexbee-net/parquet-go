package datastore

import (
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
)

// parquetColumn is to convert a store to a parquet.SchemaElement.
type parquetColumn interface {
	ParquetType() parquet.Type
	RepetitionType() parquet.FieldRepetitionType
	Params() (*ColumnParameters, error)
}

type typedColumnStore interface {
	parquetColumn

	Reset(repetitionType parquet.FieldRepetitionType)

	// Min and Max in parquet byte
	MaxValue() []byte
	MinValue() []byte

	// Should extract the value, turn it into an array and check for min and max on all values in this
	GetValues(v interface{}) ([]interface{}, error)
	SizeOf(v interface{}) int

	// the tricky append. this is a way of creating new "typed" array. the first interface is nil or an []T (T is the type,
	// not the interface) and value is from that type. the result should be always []T (array of that type)
	// exactly like the builtin append
	Append(arrayIn, value interface{}) interface{}
}

// ColumnStore is the read/write implementation for a column.
// It buffers a single column's data that is to be written to a parquet file,
// knows how to encode this data and will choose an optimal way according to
// heuristics.
// It also ensures the correct decoding of column data to be read.
type ColumnStore struct {
	typedColumnStore

	repTyp parquet.FieldRepetitionType

	Values *Dict

	DefinitionLevels *encoding.PackedArray
	RepetitionLevels *encoding.PackedArray

	encoding parquet.Encoding
	readPos  int

	allowDict bool

	Skipped bool
}

func newColumnStore(typed typedColumnStore, encoding parquet.Encoding, allowDict bool) *ColumnStore {
	return &ColumnStore{
		typedColumnStore: typed,
		encoding:         encoding,
		allowDict:        allowDict,
	}
}

func newPlainColumnStore(typed typedColumnStore) *ColumnStore {
	return newColumnStore(typed, parquet.Encoding_PLAIN, true)
}
