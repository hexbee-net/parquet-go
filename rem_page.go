// +build removeme

package parquet

import (
	"io"

	"github.com/hexbee-net/parquet/parquet"
)

// pageReader is an internal interface used only internally to read pages.
type pageReader interface {
	init(dDecoder, rDecoder getLevelDecoder, values getValueDecoderFn) error
	read(r io.Reader, ph *parquet.PageHeader, codec parquet.CompressionCodec) error

	readValues([]interface{}) (n int, dLevel *packedArray, rLevel *packedArray, err error)

	numValues() int32
}

// pageReader is an internal interface used only internally to write pages.
type pageWriter interface {
	init(schema SchemaWriter, col *Column, codec parquet.CompressionCodec) error

	write(w io.Writer) (int, int, error)
}
