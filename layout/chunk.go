package layout

import (
	"io"
	"math/bits"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/compression"
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
	"github.com/hexbee-net/parquet/source"
	"github.com/hexbee-net/parquet/types"
)

type getValueDecoderFn func(parquet.Encoding) (types.ValuesDecoder, error)
type getLevelDecoderFn func(parquet.Encoding) (levelDecoder, error)

type compressorMap map[parquet.CompressionCodec]compression.BlockCompressor

type ChunkReader struct {
	compressors map[parquet.CompressionCodec]compression.BlockCompressor
}

func NewChunkReader(compressors map[parquet.CompressionCodec]compression.BlockCompressor) *ChunkReader {
	return &ChunkReader{compressors: compressors}
}

func SkipChunk(reader io.Seeker, col *schema.Column, chunk *parquet.ColumnChunk) error {
	if err := checkColumnChunk(chunk, col); err != nil {
		return err
	}

	offset := chunk.MetaData.DataPageOffset

	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}

	offset += chunk.MetaData.TotalCompressedSize

	// Seek to the end of the chunk.
	if _, err := reader.Seek(offset, io.SeekStart); err != nil {
		return errors.WithFields(
			errors.Wrap(err, "failed to set the read index to the next chunk start"),
			errors.Fields{
				"offset": offset,
			})
	}

	return nil
}

func (r *ChunkReader) ReadChunk(src source.Reader, col *schema.Column, chunk *parquet.ColumnChunk) ([]PageReader, error) {
	if err := checkColumnChunk(chunk, col); err != nil {
		return nil, err
	}

	offset := chunk.MetaData.DataPageOffset
	if chunk.MetaData.DictionaryPageOffset != nil {
		offset = *chunk.MetaData.DictionaryPageOffset
	}

	// Seek to the beginning of the first page in the column chunk.
	if _, err := src.Seek(offset, io.SeekStart); err != nil {
		return nil, errors.WithFields(
			errors.Wrap(err, "failed to set the read index to page start"),
			errors.Fields{
				"offset": offset,
			})
	}

	reader := &offsetReader{
		inner:  src,
		offset: offset,
		count:  0,
	}

	rDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.WithFields(
				errors.New("encoding not supported for definition and repetition level"),
				errors.Fields{
					"encoding": enc.String(),
				})
		}

		dec := encoding.NewHybridDecoder(bits.Len16(col.MaxRepetitionLevel()), true)

		return &levelDecoderWrapper{
			Decoder: dec,
			max:     col.MaxRepetitionLevel(),
		}, nil
	}
	dDecoder := func(enc parquet.Encoding) (levelDecoder, error) {
		if enc != parquet.Encoding_RLE {
			return nil, errors.WithFields(
				errors.New("encoding not supported for definition and repetition level"),
				errors.Fields{
					"encoding": enc.String(),
				})
		}
		dec := encoding.NewHybridDecoder(bits.Len16(col.MaxDefinitionLevel()), true)

		return &levelDecoderWrapper{
			Decoder: dec,
			max:     col.MaxDefinitionLevel(),
		}, nil
	}

	if col.MaxRepetitionLevel() == 0 {
		rDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{
				Decoder: encoding.ConstDecoder(0),
				max:     col.MaxRepetitionLevel(),
			}, nil
		}
		dDecoder = func(parquet.Encoding) (levelDecoder, error) {
			return &levelDecoderWrapper{
				Decoder: encoding.ConstDecoder(0),
				max:     col.MaxDefinitionLevel(),
			}, nil
		}
	}

	return r.readPages(reader, col, chunk.MetaData, dDecoder, rDecoder)
}

func (r *ChunkReader) readPages(reader *offsetReader, col *schema.Column, chunkMeta *parquet.ColumnMetaData, dDecoder, rDecoder getLevelDecoderFn) ([]PageReader, error) {
	var dictPage *dictPageReader
	var pages []PageReader

	for {
		if chunkMeta.TotalCompressedSize-reader.Count() < 1 {
			break
		}

		pageHeader := &parquet.PageHeader{}
		if err := readThrift(pageHeader, reader); err != nil {
			return nil, errors.Wrap(err, "failed to read page header")
		}

		var p PageReader
		switch pageHeader.Type {
		case parquet.PageType_DICTIONARY_PAGE:
			if dictPage != nil {
				return nil, errors.New("there should be only one dictionary")
			}

			dictPage = &dictPageReader{}

			de, err := getDictValuesDecoder(col.Element())
			if err != nil {
				return nil, errors.Wrap(err, "failed to get dict value decoder")
			}

			if err := dictPage.init(de, r.compressors); err != nil {
				return nil, err
			}

			// re-use the value dictionary store
			dictPage.values = col.ColumnStore().Values.Values
			if err := dictPage.read(reader, pageHeader, chunkMeta.Codec); err != nil {
				return nil, err
			}

			// Go to the next data Page.
			// if we have a DictionaryPageOffset, we should return to DataPageOffset.
			if chunkMeta.DictionaryPageOffset != nil {
				if *chunkMeta.DictionaryPageOffset != reader.offset {
					if _, err := reader.Seek(chunkMeta.DataPageOffset, io.SeekStart); err != nil {
						return nil, errors.Wrap(err, "failed to set the read index to the start of the next page")
					}
				}
			}

			continue

		case parquet.PageType_DATA_PAGE:
			p = &dataPageReaderV1{page: page{pageHeader: pageHeader}}

		case parquet.PageType_DATA_PAGE_V2:
			p = &dataPageReaderV2{page: page{pageHeader: pageHeader}}

		default:
			return nil, errors.WithFields(
				errors.New("page type not supported"),
				errors.Fields{
					"page-type": pageHeader.Type.String(),
				})
		}

		var dictValue []interface{}
		if dictPage != nil {
			dictValue = dictPage.values
		}

		var fn = func(typ parquet.Encoding) (types.ValuesDecoder, error) {
			return getValuesDecoder(typ, col.Element(), dictValue)
		}
		if err := p.init(dDecoder, rDecoder, fn, r.compressors); err != nil {
			return nil, err
		}

		if err := p.read(reader, pageHeader, chunkMeta.Codec); err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}

	return pages, nil
}

func checkColumnChunk(chunk *parquet.ColumnChunk, col *schema.Column) error {
	if chunk.FilePath != nil {
		return errors.WithFields(
			errors.New("data is in another file"),
			errors.Fields{
				"filepath": *chunk.FilePath,
			})
	}

	c := col.Index()

	if chunk.MetaData == nil {
		return errors.WithFields(
			errors.New("missing meta-data for column"),
			errors.Fields{
				"column-index": c,
			})
	}

	if typ := *col.Element().Type; chunk.MetaData.Type != typ {
		return errors.WithFields(
			errors.New("wrong type in column chunk meta-data"),
			errors.Fields{
				"expected": typ,
				"actual":   chunk.MetaData.Type.String(),
			})
	}

	return nil
}
