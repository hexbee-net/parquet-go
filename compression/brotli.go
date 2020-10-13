package compression //nolint:dupl // it's easier to duplicate the algorithm wrappers

import (
	"bytes"
	"io/ioutil"

	"github.com/andybalholm/brotli"
	"github.com/hexbee-net/errors"
)

type Brotli struct {
}

func (c Brotli) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := brotli.NewWriter(buf)

	if _, err := w.Write(block); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c Brotli) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r := brotli.NewReader(buf)

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress Brotli data")
	}

	return ret, nil
}
