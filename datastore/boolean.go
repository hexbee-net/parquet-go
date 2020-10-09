package datastore

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

type BooleanStore struct {
	valueStore
}

// NewBooleanStore creates new column store to store boolean values.
func NewBooleanStore(enc parquet.Encoding, params *ColumnParameters) (*ColumnStore, error) {
	switch enc { //nolint:exhaustive
	case parquet.Encoding_PLAIN, parquet.Encoding_RLE:
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported on boolean type"),
			errors.Fields{
				"encoding": enc.String(),
			})
	}

	return newColumnStore(&BooleanStore{valueStore: valueStore{ColumnParameters: params}}, enc, false), nil
}

func (s *BooleanStore) ParquetType() parquet.Type {
	return parquet.Type_BOOLEAN
}

func (s *BooleanStore) Reset(repetitionType parquet.FieldRepetitionType) {
	s.repTyp = repetitionType
}

func (s *BooleanStore) MinValue() []byte {
	return nil
}

func (s *BooleanStore) MaxValue() []byte {
	return nil
}

func (s *BooleanStore) SizeOf(v interface{}) int {
	// Use zero size to make sure we never use dictionary on this.
	return 0
}

func (s *BooleanStore) GetValues(v interface{}) ([]interface{}, error) {
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

func (s *BooleanStore) Append(arrayIn, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]bool, 0, 1)
	}

	return append(arrayIn.([]bool), value.(bool))
}
