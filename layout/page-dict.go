package layout

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/types"
)

type dictPageReader struct {
	page

	values []interface{}
}

func (r *dictPageReader) init(decoder types.ValuesDecoder, compressors compressorMap) error {
	if decoder == nil {
		return errors.New("dictionary page without dictionary value encoder")
	}

	r.valuesDecoder = decoder
	r.blockReader = blockReader{compressors: compressors}
	return nil
}

func (r *dictPageReader) read(reader io.Reader, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec) error {
	if pageHeader.DictionaryPageHeader == nil {
		return errors.New("missing dictionary page header")
	}

	if pageHeader.DictionaryPageHeader.NumValues < 0 {
		return errors.WithFields(
			errors.New("negative NumValues in DICTIONARY_PAGE"),
			errors.Fields{
				"num-values": pageHeader.DictionaryPageHeader.NumValues,
			},
		)
	}

	if pageHeader.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN && pageHeader.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return errors.WithFields(
			errors.New("only Encoding_PLAIN and Encoding_PLAIN_DICTIONARY are supported for dict values encoder"),
			errors.Fields{
				"encoding": pageHeader.DictionaryPageHeader.Encoding,
			},
		)
	}

	r.valuesCount = pageHeader.DictionaryPageHeader.NumValues
	r.pageHeader = pageHeader

	dataReader, err := r.readPageBlock(reader, codec, pageHeader.GetCompressedPageSize(), pageHeader.GetUncompressedPageSize())
	if err != nil {
		return err
	}

	if cap(r.values) < int(r.valuesCount) {
		r.values = make([]interface{}, 0, r.valuesCount)
	}
	r.values = r.values[:int(r.valuesCount)]

	if err := r.valuesDecoder.Init(dataReader); err != nil {
		return errors.WithStack(err)
	}

	// no error is accepted here, even EOF
	if n, err := r.valuesDecoder.DecodeValues(r.values); err != nil {
		return errors.WithFields(
			errors.New("unexpected number of values"),
			errors.Fields{
				"expected": r.valuesCount,
				"actual":   n,
			})
	}

	return nil
}

// /////////////////////////////////////

func getDictValuesDecoder(typ *parquet.SchemaElement) (types.ValuesDecoder, error) {
	switch *typ.Type {
	case parquet.Type_BYTE_ARRAY:
		return &types.ByteArrayPlainDecoder{}, nil

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.Errorf("type %s with nil type len", typ)
		}
		return &types.ByteArrayPlainDecoder{Length: int(*typ.TypeLength)}, nil

	case parquet.Type_FLOAT:
		return &types.FloatPlainDecoder{}, nil

	case parquet.Type_DOUBLE:
		return &types.DoublePlainDecoder{}, nil

	case parquet.Type_INT32:
		var unsigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_8 || *typ.ConvertedType == parquet.ConvertedType_UINT_16 || *typ.ConvertedType == parquet.ConvertedType_UINT_32 {
				unsigned = true
			}
		}

		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unsigned = true
		}

		return &types.Int32PlainDecoder{Unsigned: unsigned}, nil

	case parquet.Type_INT64:
		var unsigned bool
		if typ.ConvertedType != nil {
			if *typ.ConvertedType == parquet.ConvertedType_UINT_64 {
				unsigned = true
			}
		}

		if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
			unsigned = true
		}

		return &types.Int64PlainDecoder{Unsigned: unsigned}, nil

	case parquet.Type_INT96:
		return &types.Int96PlainDecoder{}, nil
	}

	return nil, errors.WithFields(
		errors.New("type not supported for dict value encoder"),
		errors.Fields{
			"type": typ,
		})
}

func getValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (types.ValuesDecoder, error) {
	// Change the deprecated value
	if pageEncoding == parquet.Encoding_PLAIN_DICTIONARY {
		pageEncoding = parquet.Encoding_RLE_DICTIONARY
	}

	switch *typ.Type {
	case parquet.Type_BOOLEAN:
		return getBooleanValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_INT32:
		return getInt32ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT64:
		return getInt64ValuesDecoder(pageEncoding, typ, dictValues)

	case parquet.Type_INT96:
		return getInt96ValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_FLOAT:
		return getFloatValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_DOUBLE:
		return getDoubleValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_BYTE_ARRAY:
		return getByteArrayValuesDecoder(pageEncoding, dictValues)

	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if typ.TypeLength == nil {
			return nil, errors.WithFields(
				errors.New("type with nil type length"),
				errors.Fields{
					"type": typ.Type,
				})
		}

		return getFixedLenByteArrayValuesDecoder(pageEncoding, int(*typ.TypeLength), dictValues)

	default:
		return nil, errors.WithFields(
			errors.New("unsupported type"),
			errors.Fields{
				"type": typ.Type,
			})
	}
}

func getBooleanValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.BooleanPlainDecoder{}, nil
	case parquet.Encoding_RLE:
		return &types.BooleanRLEDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for boolean"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getInt32ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (types.ValuesDecoder, error) {
	var unsigned bool

	if typ.ConvertedType != nil {
		if *typ.ConvertedType == parquet.ConvertedType_UINT_8 || *typ.ConvertedType == parquet.ConvertedType_UINT_16 || *typ.ConvertedType == parquet.ConvertedType_UINT_32 {
			unsigned = true
		}
	}

	if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
		unsigned = true
	}

	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.Int32PlainDecoder{Unsigned: unsigned}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &types.Int32DeltaBPDecoder{Unsigned: unsigned}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for int32"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getInt64ValuesDecoder(pageEncoding parquet.Encoding, typ *parquet.SchemaElement, dictValues []interface{}) (types.ValuesDecoder, error) {
	var unsigned bool

	if typ.ConvertedType != nil {
		if *typ.ConvertedType == parquet.ConvertedType_UINT_64 {
			unsigned = true
		}
	}

	if typ.LogicalType != nil && typ.LogicalType.INTEGER != nil && !typ.LogicalType.INTEGER.IsSigned {
		unsigned = true
	}

	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.Int64PlainDecoder{Unsigned: unsigned}, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return &types.Int64DeltaBPDecoder{Unsigned: unsigned}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for int32"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getInt96ValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.Int96PlainDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for int96"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getFloatValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.FloatPlainDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for float"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getDoubleValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.DoublePlainDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for double"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getByteArrayValuesDecoder(pageEncoding parquet.Encoding, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.ByteArrayPlainDecoder{}, nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return &types.ByteArrayDeltaLengthDecoder{}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &types.ByteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for binary"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}

func getFixedLenByteArrayValuesDecoder(pageEncoding parquet.Encoding, len int, dictValues []interface{}) (types.ValuesDecoder, error) {
	switch pageEncoding {
	case parquet.Encoding_PLAIN:
		return &types.ByteArrayPlainDecoder{Length: len}, nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return &types.ByteArrayDeltaDecoder{}, nil
	case parquet.Encoding_RLE_DICTIONARY:
		return &types.DictDecoder{Values: dictValues}, nil
	default:
		return nil, errors.WithFields(
			errors.New("encoding not supported for fixed_len_byte_array"),
			errors.Fields{
				"encoding": pageEncoding.String(),
			})
	}
}
