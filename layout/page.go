package layout

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema"
	"github.com/hexbee-net/parquet/types"
)

// PageReader is an internal interface used only internally to read pages.
type PageReader interface {
	init(dDecoder, rDecoder getLevelDecoderFn, values getValueDecoderFn, compressors compressorMap) error
	read(reader io.Reader, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec) error

	ReadValues(values []interface{}) (n int, dLevel *encoding.PackedArray, rLevel *encoding.PackedArray, err error)

	NumValues() int32
}

// PageWriter is an internal interface used only internally to write pages.
type PageWriter interface {
	init(schema schema.Writer, col *schema.Column, codec parquet.CompressionCodec) error
	write(w io.Writer) (int, int, error)
}

type page struct {
	PageReader

	pageHeader    *parquet.PageHeader
	valuesCount   int32
	valuesDecoder types.ValuesDecoder
	blockReader   blockReader
}

func (p *page) readPageBlock(in io.Reader, codec parquet.CompressionCodec, compressedSize int32, uncompressedSize int32) (io.Reader, error) {
	if compressedSize < 0 || uncompressedSize < 0 {
		return nil, errors.WithFields(
			errors.New("invalid page data size"),
			errors.Fields{
				"compressed-size":   compressedSize,
				"uncompressed-size": uncompressedSize,
			})
	}

	return p.blockReader.readBlockData(in, codec, compressedSize, uncompressedSize)
}
