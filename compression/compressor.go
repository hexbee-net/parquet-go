package compression

type BlockCompressor interface {
	CompressBlock(block []byte) ([]byte, error)
	DecompressBlock(block []byte) ([]byte, error)
}
