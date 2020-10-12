package types

import (
	"io"

	"github.com/hexbee-net/errors"
)

// Encoder /////////////////////////////

type Int96PlainEncoder struct {
	writer io.Writer
}

func (e Int96PlainEncoder) Init(writer io.Writer) error {
	e.writer = writer

	return nil
}

func (e Int96PlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]byte, len(values)*12)
	for j := range values {
		i96 := values[j].([12]byte)
		copy(data[j*12:], i96[:])
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
		var data [12]byte

		// this one is a little tricky do not use ReadFull here
		n, err := d.reader.Read(data[:])

		// make sure we handle the read data first then handle the error
		if n == 12 {
			dest[idx] = data
			idx++
		}

		if err != nil && (n == 0 || n == 12) {
			return idx, err
		}

		if err != nil {
			return idx, errors.Wrap(err, "not enough byte to read the Int96")
		}
	}

	return len(dest), nil
}
