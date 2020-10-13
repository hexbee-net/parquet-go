package types

import (
	"io"

	"github.com/hexbee-net/errors"
)

const sizeInt96 = 12

// Encoder /////////////////////////////

type Int96PlainEncoder struct {
	writer io.Writer
}

func (e Int96PlainEncoder) Init(writer io.Writer) error {
	e.writer = writer

	return nil
}

func (e Int96PlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]byte, len(values)*sizeInt96)

	for j := range values {
		i96 := values[j].([sizeInt96]byte)
		copy(data[j*sizeInt96:], i96[:])
	}

	return writeFull(e.writer, data)
}

func (e Int96PlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type Int96PlainDecoder struct {
	reader io.Reader
}

func (d Int96PlainDecoder) Init(reader io.Reader) error {
	d.reader = reader

	return nil
}

func (d Int96PlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	idx := 0

	for range dest {
		var data [sizeInt96]byte

		// this one is a little tricky do not use ReadFull here
		n, err := d.reader.Read(data[:])

		// make sure we handle the read data first then handle the error
		if n == sizeInt96 {
			dest[idx] = data
			idx++
		}

		if err != nil && (n == 0 || n == sizeInt96) {
			return idx, err
		}

		if err != nil {
			return idx, errors.Wrap(err, "not enough byte to read the Int96")
		}
	}

	return len(dest), nil
}
