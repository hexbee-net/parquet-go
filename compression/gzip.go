package compression

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/hexbee-net/errors"
)

type GZip struct {
}

func (c GZip) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)

	if _, err := w.Write(block); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c GZip) DecompressBlock(block []byte) ([]byte, error) {
	buf := bytes.NewReader(block)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress GZIP data")
	}

	return ret, r.Close()
}
