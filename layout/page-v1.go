package layout

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
)

type dataPageReaderV1 struct {
	page

	encoding          parquet.Encoding
	definitionDecoder levelDecoder
	repetitionDecoder levelDecoder
	valueDecoderFn    getValueDecoderFn
	position          int
}

func (r *dataPageReaderV1) init(dDecoderFn, rDecoderFn getLevelDecoderFn, valueDecoderFn getValueDecoderFn, compressors compressorMap) (err error) {
	r.definitionDecoder, err = dDecoderFn(r.pageHeader.DataPageHeader.DefinitionLevelEncoding)
	if err != nil {
		return errors.WithStack(err)
	}

	r.repetitionDecoder, err = rDecoderFn(r.pageHeader.DataPageHeader.RepetitionLevelEncoding)
	if err != nil {
		return errors.WithStack(err)
	}

	r.valueDecoderFn = valueDecoderFn
	r.position = 0

	r.blockReader = blockReader{compressors: compressors}

	return nil
}

func (r *dataPageReaderV1) read(reader io.Reader, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec) error {
	if pageHeader.DataPageHeader == nil {
		return errors.New("missing data page header")
	}

	if r.valuesCount = pageHeader.DataPageHeader.NumValues; r.valuesCount < 0 {
		return errors.WithFields(
			errors.New("negative NumValues in DATA_PAGE"),
			errors.Fields{
				"num-values": r.valuesCount,
			})
	}

	dataReader, err := r.readPageBlock(reader, codec, pageHeader.GetCompressedPageSize(), pageHeader.GetUncompressedPageSize())
	if err != nil {
		return errors.WithStack(err)
	}

	r.encoding = pageHeader.DataPageHeader.Encoding
	r.pageHeader = pageHeader

	if r.valuesDecoder, err = r.valueDecoderFn(r.encoding); err != nil {
		return errors.WithFields(
			errors.New("failed to get value decoder for encoding"),
			errors.Fields{
				"encoding": r.encoding,
			})
	}

	if err := r.repetitionDecoder.InitSize(dataReader); err != nil {
		return errors.Wrap(err, "failed to initialize repetition decoder")
	}

	if err := r.definitionDecoder.InitSize(dataReader); err != nil {
		return errors.Wrap(err, "failed to initialize definition decoder")
	}

	return r.valuesDecoder.Init(dataReader)
}

func (r *dataPageReaderV1) ReadValues(values []interface{}) (n int, dLevel *encoding.PackedArray, rLevel *encoding.PackedArray, err error) {
	size := len(values)
	if rem := int(r.valuesCount) - r.position; rem < size {
		size = rem
	}

	if size == 0 {
		return 0, nil, nil, nil
	}

	rLevel, _, err = decodePackedArray(r.repetitionDecoder, size)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "read repetition levels failed")
	}

	var notNull int
	dLevel, notNull, err = decodePackedArray(r.definitionDecoder, size)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "read definition levels failed")
	}

	if notNull != 0 {
		if n, err := r.valuesDecoder.DecodeValues(values[:notNull]); err != nil {
			return 0, nil, nil, errors.WithFields(
				errors.New("read values from page failed"),
				errors.Fields{
					"expected": notNull,
					"actual":   n,
				})
		}
	}
	r.position += size
	return size, dLevel, rLevel, nil
}

func (r *dataPageReaderV1) NumValues() int32 {
	return r.valuesCount
}
