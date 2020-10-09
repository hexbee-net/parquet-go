package datastore

import (
	"encoding/binary"
	"math"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

const sizeFloat = 4

type FloatStore struct {
	valueStore
	min float32
	max float32
}

// NewFloatStore creates a new column store to store float (float32) values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewFloatStore(enc parquet.Encoding, allowDict bool, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on float type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return newColumnStore(&FloatStore{valueStore: valueStore{ColumnParameters: params}}, enc, allowDict), nil
}

func (s *FloatStore) ParquetType() parquet.Type {
	return parquet.Type_FLOAT
}

func (s *FloatStore) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
	s.min = math.MaxFloat32
	s.max = -math.MaxFloat32
}

func (s *FloatStore) MinValue() []byte {
	if s.min == math.MaxFloat32 {
		return nil
	}

	ret := make([]byte, sizeFloat)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(s.min))

	return ret
}

func (s *FloatStore) MaxValue() []byte {
	if s.max == -math.MaxFloat32 {
		return nil
	}

	ret := make([]byte, sizeFloat)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(s.max))

	return ret
}

func (s *FloatStore) SizeOf(v interface{}) int {
	return sizeFloat
}

func (s *FloatStore) GetValues(v interface{}) ([]interface{}, error) { //nolint:dupl // duplication is the easiest way without generics
	var values []interface{}

	switch typed := v.(type) {
	case float32:
		s.setMinMax(typed)
		values = []interface{}{typed}

	case []float32:
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
			errors.New("unsupported type for storing in float32 column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *FloatStore) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]float32, 0, 1)
	}

	return append(arrayIn.([]float32), value.(float32))
}

func (s *FloatStore) setMinMax(n float32) {
	if n < s.min {
		s.min = n
	}

	if n > s.max {
		s.max = n
	}
}
