package datastore

import (
	"encoding/binary"
	"math"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

const sizeDouble = 8

type DoubleStore struct {
	valueStore
	min float64
	max float64
}

// NewDoubleStore creates a new column store to store double (float64) values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewDoubleStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on double type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return newColumnStore(&DoubleStore{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

func (s *DoubleStore) ParquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (s *DoubleStore) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType

	s.min = math.MaxFloat64
	s.max = -math.MaxFloat64
}

func (s *DoubleStore) MinValue() []byte {
	if s.min == math.MaxFloat64 {
		return nil
	}

	ret := make([]byte, sizeDouble)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(s.min))

	return ret
}

func (s *DoubleStore) MaxValue() []byte {
	if s.max == -math.MaxFloat64 {
		return nil
	}

	ret := make([]byte, sizeDouble)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(s.max))

	return ret
}

func (s *DoubleStore) SizeOf(v interface{}) int {
	return sizeDouble
}

func (s *DoubleStore) GetValues(v interface{}) ([]interface{}, error) { //nolint:dupl // duplication is the easiest way without generics
	var values []interface{}

	switch typed := v.(type) {
	case float64:
		s.setMinMax(typed)
		values = []interface{}{typed}

	case []float64:
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
			errors.New("unsupported type for storing in float64 column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *DoubleStore) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float64, 0, 1)
	}

	return append(arrayIn.([]float64), value.(float64))
}

func (s *DoubleStore) setMinMax(n float64) {
	if n < s.min {
		s.min = n
	}

	if n > s.max {
		s.max = n
	}
}
