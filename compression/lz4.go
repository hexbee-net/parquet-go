package compression //nolint:dupl // it's easier to duplicate the algorithm wrappers

import (
	"bytes"
	"io/ioutil"

	"github.com/hexbee-net/errors"
	"github.com/pierrec/lz4"
)

type LZ4 struct {
}

func (c LZ4) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := lz4.NewWriter(buf)

	if _, err := w.Write(block); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c LZ4) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r := lz4.NewReader(buf)

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress LZ4 data")
	}

	return ret, nil
}
