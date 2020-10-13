package datastore

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

type ByteArrayStore struct {
	valueStore
	min []byte
	max []byte
}

// NewByteArrayStore creates a new column store to store byte arrays. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewByteArrayStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive // supported encoding only
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on byte-array type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return NewColumnStore(&ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

// NewFixedByteArrayStore creates a new column store to store fixed size byte arrays. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewFixedByteArrayStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive // supported encoding only
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Encoding_DELTA_BYTE_ARRAY:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on fixed byte-array type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	if params.TypeLength == nil {
		return nil, errors.New("no length provided")
	}

	if *params.TypeLength <= 0 {
		return nil, errors.Errorf("fix length with len %d is not possible", *params.TypeLength)
	}

	return NewColumnStore(&ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

func (s *ByteArrayStore) ParquetType() parquet.Type {
	if s.TypeLength != nil && *s.TypeLength > 0 {
		return parquet.Type_FIXED_LEN_BYTE_ARRAY
	}

	return parquet.Type_BYTE_ARRAY
}

func (s *ByteArrayStore) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
	s.min = nil
	s.max = nil
}

func (s *ByteArrayStore) MinValue() []byte {
	return s.min
}

func (s *ByteArrayStore) MaxValue() []byte {
	return s.max
}

func (s *ByteArrayStore) SizeOf(v interface{}) int {
	return len(v.([]byte))
}

func (s *ByteArrayStore) GetValues(v interface{}) ([]interface{}, error) { //nolint:dupl // duplication is the easiest way without generics
	var values []interface{}

	switch typed := v.(type) {
	case []byte:
		values = []interface{}{typed}

	case [][]byte:
		if s.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.New("the value is not repeated but it is an array")
		}

		values = make([]interface{}, len(typed))

		for j := range typed {
			values[j] = typed[j]
		}
	default:
		return nil, errors.WithFields(
			errors.New("unsupported type for storing in []byte column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *ByteArrayStore) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([][]byte, 0, 1)
	}

	return append(arrayIn.([][]byte), value.([]byte))
}
