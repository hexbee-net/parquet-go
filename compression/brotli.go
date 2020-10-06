package compression

import (
	"bytes"
	"io/ioutil"

	"github.com/google/brotli/go/cbrotli"
	"github.com/hexbee-net/errors"
)

type Brotli struct {
	cbrotli.WriterOptions
}

func (c Brotli) CompressBlock(block []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	w := cbrotli.NewWriter(buf, c.WriterOptions)

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
	r := cbrotli.NewReader(buf)

	ret, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decompress Brotli data")
	}

	return ret, r.Close()
}
