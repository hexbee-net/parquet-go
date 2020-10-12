package datastore

import (
	"encoding/binary"
	"math"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
)

const sizeInt32 = 4

type Int32Store struct {
	valueStore
	min int32
	max int32
}

// NewInt32Store create a new column store to store int32 values.
// If allowDict is true, then using a dictionary is considered by
// the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewInt32Store(enc parquet.Encoding, allowDict bool, params *schema.ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_BINARY_PACKED:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on int32 type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return newColumnStore(&Int32Store{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

func (s *Int32Store) ParquetType() parquet.Type {
	return parquet.Type_INT32
}

func (s *Int32Store) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
	s.min = math.MaxInt32
	s.max = math.MinInt32
}

func (s *Int32Store) MinValue() []byte {
	if s.min == math.MaxInt32 {
		return nil
	}

	ret := make([]byte, sizeInt32)
	binary.LittleEndian.PutUint32(ret, uint32(s.min))

	return ret
}

func (s *Int32Store) MaxValue() []byte {
	if s.max == math.MinInt32 {
		return nil
	}

	ret := make([]byte, sizeInt32)
	binary.LittleEndian.PutUint32(ret, uint32(s.max))

	return ret
}

func (s *Int32Store) SizeOf(v interface{}) int {
	return sizeInt32
}

func (s *Int32Store) GetValues(v interface{}) ([]interface{}, error) { //nolint:dupl // duplication is the easiest way without generics
	var values []interface{}

	switch typed := v.(type) {
	case int32:
		s.setMinMax(typed)
		values = []interface{}{typed}

	case []int32:
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
			errors.New("unsupported type for storing in int32 column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *Int32Store) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]int32, 0, 1)
	}

	return append(arrayIn.([]int32), value.(int32))
}

func (s *Int32Store) setMinMax(n int32) {
	if n < s.min {
		s.min = n
	}

	if n > s.max {
		s.max = n
	}
}
