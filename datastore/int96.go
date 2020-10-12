package datastore

import (
	"bytes"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
)

const sizeInt96 = 12

type Int96Store struct {
	ByteArrayStore
}

// NewInt96Store creates a new column store to store int96 values. If allowDict is true,
// then using a dictionary is considered by the column store depending on its heuristics.
// If allowDict is false, a dictionary will never be used to encode the data.
func NewInt96Store(enc parquet.Encoding, allowDict bool, params *schema.ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on int96 type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	store := &Int96Store{}
	store.ColumnParameters = params

	return newColumnStore(store, enc, allowDict), nil
}

func (s *Int96Store) ParquetType() parquet.Type {
	return parquet.Type_INT96
}

func (s *Int96Store) SizeOf(v interface{}) int {
	return sizeInt96
}

func (s *Int96Store) GetValues(v interface{}) ([]interface{}, error) {
	var values []interface{}

	switch typed := v.(type) {
	case [12]byte:
		if err := s.setMinMax(typed[:]); err != nil {
			return nil, err
		}

		values = []interface{}{typed}

	case [][12]byte:
		if s.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}

		values = make([]interface{}, len(typed))

		for j := range typed {
			if err := s.setMinMax(typed[j][:]); err != nil {
				return nil, err
			}

			values[j] = typed[j]
		}

	default:
		return nil, errors.Errorf("unsupported type for storing in Int96 column: %T => %+v", v, v)
	}

	return values, nil
}

func (s *Int96Store) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([][12]byte, 0, 1)
	}

	return append(arrayIn.([][12]byte), value.([12]byte))
}

func (s *Int96Store) setMinMax(n []byte) error {
	if s.TypeLength != nil && *s.TypeLength > 0 && int32(len(n)) != *s.TypeLength {
		return errors.WithFields(
			errors.New("invalid data size"),
			errors.Fields{
				"expected": *s.TypeLength,
				"actual":   len(n),
			})
	}

	// For nil value there is no need to set the min/max
	if n == nil {
		return nil
	}

	if s.max == nil || s.min == nil {
		s.min = n
		s.max = n

		return nil
	}

	if bytes.Compare(n, s.min) < 0 {
		s.min = n
	}

	if bytes.Compare(n, s.max) > 0 {
		s.max = n
	}

	return nil
}
