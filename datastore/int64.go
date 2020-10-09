package datastore

import (
	"encoding/binary"
	"math"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

const sizeInt64 = 8

type Int64Store struct {
	valueStore
	min int64
	max int64
}

// NewInt64Store creates a new column store to store int64 values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewInt64Store(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on int64 type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return newColumnStore(&Int64Store{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

func (s *Int64Store) ParquetType() parquet.Type {
	return parquet.Type_INT64
}

func (s *Int64Store) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
	s.min = math.MaxInt64
	s.max = math.MinInt64
}

func (s *Int64Store) MinValue() []byte {
	if s.min == math.MaxInt64 {
		return nil
	}

	ret := make([]byte, sizeInt64)
	binary.LittleEndian.PutUint64(ret, uint64(s.min))

	return ret
}

func (s *Int64Store) MaxValue() []byte {
	if s.max == math.MinInt64 {
		return nil
	}

	ret := make([]byte, sizeInt64)
	binary.LittleEndian.PutUint64(ret, uint64(s.max))

	return ret
}

func (s *Int64Store) SizeOf(v interface{}) int {
	return sizeInt64
}

func (s *Int64Store) GetValues(v interface{}) ([]interface{}, error) { //nolint:dupl // duplication is the easiest way without generics
	var values []interface{}

	switch typed := v.(type) {
	case int64:
		s.setMinMax(typed)
		values = []interface{}{typed}

	case []int64:
		if s.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.New("the value is not repeated but it is an array")
		}

		values = make([]interface{}, len(typed))

		for j := range typed {
			s.setMinMax(typed[j])
			values[j] = typed[j]
		}

	default:
		return nil, errors.WithFields(
			errors.New("unsupported type for storing in int64 column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *Int64Store) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]int64, 0, 1)
	}

	return append(arrayIn.([]int64), value.(int64))
}

func (s *Int64Store) setMinMax(n int64) {
	if n < s.min {
		s.min = n
	}

	if n > s.max {
		s.max = n
	}
}
