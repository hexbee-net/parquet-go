package layout

import (
	"bytes"
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
)

type dataPageReaderV2 struct {
	page

	encoding          parquet.Encoding
	definitionDecoder levelDecoder
	repetitionDecoder levelDecoder
	valueDecoderFn    getValueDecoderFn
	position          int
}

func (r *dataPageReaderV2) init(dDecoder, rDecoder getLevelDecoderFn, valueDecoderFn getValueDecoderFn, compressors compressorMap) (err error) {
	// Page v2 dose not have any encoding for the levels
	r.definitionDecoder, err = dDecoder(parquet.Encoding_RLE)
	if err != nil {
		return errors.WithStack(err)
	}

	r.repetitionDecoder, err = rDecoder(parquet.Encoding_RLE)
	if err != nil {
		return errors.WithStack(err)
	}

	r.valueDecoderFn = valueDecoderFn
	r.position = 0

	r.blockReader = blockReader{compressors: compressors}

	return nil
}

func (r *dataPageReaderV2) read(reader io.Reader, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec) (err error) {
	// 1- Uncompressed size is affected by the level lens.
	// 2- In page V2 the rle size is in header, not in level stream
	if pageHeader.DataPageHeaderV2 == nil {
		return errors.New("missing data page header")
	}

	if r.valuesCount = pageHeader.DataPageHeaderV2.NumValues; r.valuesCount < 0 {
		return errors.WithFields(
			errors.New("negative NumValues in DATA_PAGE_V2"),
			errors.Fields{
				"num-values": r.valuesCount,
			})
	}

	if pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength < 0 {
		return errors.WithFields(
			errors.New("invalid RepetitionLevelsByteLength"),
			errors.Fields{
				"value": pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength,
			})
	}
	if pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength < 0 {
		return errors.WithFields(
			errors.New("invalid DefinitionLevelsByteLength"),
			errors.Fields{
				"value": pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength,
			})
	}
	r.encoding = pageHeader.DataPageHeaderV2.Encoding
	r.pageHeader = pageHeader

	if r.valuesDecoder, err = r.valueDecoderFn(r.encoding); err != nil {
		return err
	}

	// Its safe to call this {r,d}Decoder later, since the stream they operate on are in memory
	levelsSize := pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength + pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength

	// read both level size
	if levelsSize > 0 {
		data := make([]byte, levelsSize)
		n, err := io.ReadFull(reader, data)
		if err != nil {
			return errors.Wrapf(err, "need to read %d byte but there was only %d byte", levelsSize, n)
		}

		if pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength > 0 {
			if err := r.repetitionDecoder.Init(bytes.NewReader(data[:int(pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength)])); err != nil {
				return errors.Wrap(err, "failed to initialize repetition decoder")
			}
		}

		if pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength > 0 {
			if err := r.definitionDecoder.Init(bytes.NewReader(data[int(pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength):])); err != nil {
				return errors.Wrap(err, "failed to initialize definition decoder")
			}
		}
	}

	dataReader, err := r.readPageBlock(reader, codec, pageHeader.GetCompressedPageSize()-levelsSize, pageHeader.GetUncompressedPageSize()-levelsSize)
	if err != nil {
		return err
	}

	return r.valuesDecoder.Init(dataReader)
}

func (r *dataPageReaderV2) readValues(values []interface{}) (n int, dLevel *encoding.PackedArray, rLevel *encoding.PackedArray, err error) {
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

func (r *dataPageReaderV2) numValues() int32 {
	return r.valuesCount
}
