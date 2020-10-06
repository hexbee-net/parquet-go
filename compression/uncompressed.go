package compression

type Uncompressed struct {
}

func (c Uncompressed) CompressBlock(block []byte) ([]byte, error) {
	return block, nil
}

func (c Uncompressed) DecompressBlock(block []byte) ([]byte, error) {
	return block, nil
}
