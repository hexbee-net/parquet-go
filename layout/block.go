package layout

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/compression"
	"github.com/hexbee-net/parquet/parquet"
)

type blockReader struct {
	compressors map[parquet.CompressionCodec]compression.BlockCompressor
}

func (r *blockReader) readBlockData(in io.Reader, codec parquet.CompressionCodec, compressedSize, uncompressedSize int32) (io.Reader, error) {
	buf, err := ioutil.ReadAll(io.LimitReader(in, int64(compressedSize)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to read block data")
	}

	if len(buf) != int(compressedSize) {
		return nil, errors.WithFields(
			errors.New("invalid size for compressed data"),
			errors.Fields{
				"expected": compressedSize,
				"actual":   len(buf),
			})
	}

	res, err := r.decompressBlock(buf, codec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress block")
	}

	if len(res) != int(uncompressedSize) {
		return nil, errors.WithFields(
			errors.New("invalid size for decompressed data"),
			errors.Fields{
				"expected": uncompressedSize,
				"actual":   len(res),
			})
	}

	return bytes.NewReader(res), nil
}

func (r *blockReader) compressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	c, ok := r.compressors[method]
	if !ok {
		return nil, errors.WithFields(
			errors.New("compression method not supported"),
			errors.Fields{
				"method": method.String(),
			})
	}

	return c.CompressBlock(block)
}

func (r *blockReader) decompressBlock(block []byte, method parquet.CompressionCodec) ([]byte, error) {
	c, ok := r.compressors[method]
	if !ok {
		return nil, errors.WithFields(
			errors.New("compression method not supported"),
			errors.Fields{
				"method": method.String(),
			})
	}

	return c.DecompressBlock(block)
}
