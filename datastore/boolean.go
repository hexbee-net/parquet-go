package datastore

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

type BooleanStore struct {
	repTyp parquet.FieldRepetitionType
	*ColumnParameters
}

func (s *BooleanStore) params() (*ColumnParameters, error) {
	if s.ColumnParameters == nil {
		return nil, errors.New("ColumnParameters is nil")
	}

	return s.ColumnParameters, nil
}

func (s *BooleanStore) ParquetType() parquet.Type {
	return parquet.Type_BOOLEAN
}

func (s *BooleanStore) repetitionType() parquet.FieldRepetitionType {
	return s.repTyp
}

func (s *BooleanStore) reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
}

func (s *BooleanStore) maxValue() []byte {
	return nil
}

func (s *BooleanStore) minValue() []byte {
	return nil
}

func (s *BooleanStore) sizeOf(v interface{}) int {
	// Use zero size to make sure we never use dictionary on this.
	return 0
}

func (s *BooleanStore) getValues(v interface{}) ([]interface{}, error) {
	var values []interface{}

	switch typed := v.(type) {
	case bool:
		values = []interface{}{typed}

	case []bool:
		if s.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}

		values = make([]interface{}, len(typed))

		for j := range typed {
			values[j] = typed[j]
		}

	default:
		return nil, errors.WithFields(
			errors.New("type is not supported in boolean column"),
			errors.Fields{
				"value": v,
			})
	}

	return values, nil
}

func (s *BooleanStore) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]bool, 0, 1)
	}

	return append(arrayIn.([]bool), value.(bool))
}
