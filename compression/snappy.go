package compression

import "github.com/golang/snappy"

type Snappy struct {
}

func (c Snappy) CompressBlock(block []byte) ([]byte, error) {
	return snappy.Encode(nil, block), nil
}

func (c Snappy) DecompressBlock(block []byte) ([]byte, error) {
	return snappy.Decode(nil, block)
}
