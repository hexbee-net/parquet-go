package compression

import (
	"bytes"
	"io/ioutil"

	"github.com/hexbee-net/errors"
	"github.com/klauspost/compress/zstd"
)

type ZStd struct {
}

func (c ZStd) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}

	w, err := zstd.NewWriter(buf)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(block); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c ZStd) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)

	r, err := zstd.NewReader(buf)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress ZSTD data")
	}

	return ret, nil
}
