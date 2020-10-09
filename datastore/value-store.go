package datastore

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

// ColumnParameters contains common parameters related to a column.
type ColumnParameters struct {
	LogicalType   *parquet.LogicalType
	ConvertedType *parquet.ConvertedType
	TypeLength    *int32
	FieldID       *int32
	Scale         *int32
	Precision     *int32
}

// /////////////////////////////////////////////////////////////////////////////

type valueStore struct {
	*ColumnParameters
	repTyp parquet.FieldRepetitionType
}

func (s *valueStore) Params() (*ColumnParameters, error) {
	if s.ColumnParameters == nil {
		return nil, errors.New("missing ColumnParameters")
	}

	return s.ColumnParameters, nil
}

func (s *valueStore) RepetitionType() parquet.FieldRepetitionType {
	return s.repTyp
}

func GetValuesStore(typ *parquet.SchemaElement) (*ColumnStore, error) {
	params := &ColumnParameters{
		LogicalType:   typ.LogicalType,
		ConvertedType: typ.ConvertedType,
		TypeLength:    typ.TypeLength,
		Scale:         typ.Scale,
		Precision:     typ.Precision,
	}

	switch *typ.Type { //nolint:exhaustive
	case parquet.Type_BOOLEAN:
		return newPlainColumnStore(&BooleanStore{valueStore: valueStore{ColumnParameters: params}}), nil
	case parquet.Type_BYTE_ARRAY:
		return newPlainColumnStore(&ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.WithFields(
				errors.New("type has nil type length"),
				errors.Fields{
					"type": typ.Type.String(),
				})
		} else {
			return newPlainColumnStore(&ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}), nil
		}

	case parquet.Type_FLOAT:
		return newPlainColumnStore(&FloatStore{valueStore: valueStore{ColumnParameters: params}}), nil
	case parquet.Type_DOUBLE:
		return newPlainColumnStore(&DoubleStore{valueStore: valueStore{ColumnParameters: params}}), nil

	case parquet.Type_INT32:
		return newPlainColumnStore(&Int32Store{valueStore: valueStore{ColumnParameters: params}}), nil
	case parquet.Type_INT64:
		return newPlainColumnStore(&Int64Store{valueStore: valueStore{ColumnParameters: params}}), nil
	case parquet.Type_INT96:
		return newPlainColumnStore(&Int96Store{ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}}), nil

	default:
		return nil, errors.WithFields(
			errors.New("unsupported type"),
			errors.Fields{
				"type": typ.Type.String(),
			})
	}
}
