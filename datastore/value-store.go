package datastore

import (
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
)

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

	switch *typ.Type { //nolint:exhaustive // supported types only
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
		}

		return newPlainColumnStore(&ByteArrayStore{valueStore: valueStore{ColumnParameters: params}}), nil

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

func GetColumnStore(elem *parquet.SchemaElement, params *ColumnParameters) (colStore *ColumnStore, err error) {
	if elem.Type == nil {
		return nil, nil
	}

	typ := elem.GetType()

	switch typ { //nolint:exhaustive // supported types only
	case parquet.Type_BYTE_ARRAY:
		colStore, err = NewByteArrayStore(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_FLOAT:
		colStore, err = NewFloatStore(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_DOUBLE:
		colStore, err = NewDoubleStore(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_BOOLEAN:
		colStore, err = NewBooleanStore(parquet.Encoding_PLAIN, params)
	case parquet.Type_INT32:
		colStore, err = NewInt32Store(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_INT64:
		colStore, err = NewInt64Store(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_INT96:
		colStore, err = NewInt96Store(parquet.Encoding_PLAIN, true, params)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		colStore, err = NewFixedByteArrayStore(parquet.Encoding_PLAIN, true, params)
	default:
		return nil, errors.WithFields(
			errors.New("type not supported by column store"),
			errors.Fields{
				"type": typ.String(),
			})
	}

	if err != nil {
		return nil, errors.WithFields(
			errors.Wrap(err, "failed to create column store"),
			errors.Fields{
				"type": typ.String(),
			})
	}

	return colStore, nil
}
