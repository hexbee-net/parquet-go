// +build removeme

package parquet

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/types"
)

// Reader //////////////////////////////

type dictPageReader struct {
	ph *parquet.PageHeader

	numValues int32
	enc       types.ValuesDecoder

	values []interface{}
}

func (r *dictPageReader) init(decoder types.ValuesDecoder) error {
	if decoder == nil {
		return errors.New("dictionary page without dictionary value encoder")
	}

	r.enc = decoder
	return nil
}

func (r *dictPageReader) read(reader io.Reader, ph *parquet.PageHeader, codec parquet.CompressionCodec) error {
	if ph.DictionaryPageHeader == nil {
		return errors.New("missing dictionary page header")
	}

	if ph.DictionaryPageHeader.NumValues < 0 {
		return errors.WithFields(
			errors.New("negative NumValues in DICTIONARY_PAGE"),
			errors.Fields{
				"num-values": ph.DictionaryPageHeader.NumValues,
			},
		)
	}

	if ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN && ph.DictionaryPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return errors.WithFields(
			errors.New("only Encoding_PLAIN and Encoding_PLAIN_DICTIONARY is supported for dict values encoder"),
			errors.Fields{
				"encoding": ph.DictionaryPageHeader.Encoding,
			},
		)
	}

	r.numValues = ph.DictionaryPageHeader.NumValues
	r.ph = ph

	dataReader, err := readPageBlock(r, codec, ph.GetCompressedPageSize(), ph.GetUncompressedPageSize())
}

func (r *dictPageReader) readValues(i []interface{}) (n int, dLevel *interface{}, rLevel *interface{}, err error) {
	panic("implement me")
}
