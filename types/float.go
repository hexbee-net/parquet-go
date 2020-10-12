package types

import (
	"encoding/binary"
	"io"
	"math"
)

// Encoder /////////////////////////////

type FloatPlainEncoder struct {
	writer io.Writer
}

func (f FloatPlainEncoder) Init(writer io.Writer) error {
	f.writer = writer

	return nil
}

func (f FloatPlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(f.writer, binary.LittleEndian, data)
}

func (f FloatPlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type FloatPlainDecoder struct {
	reader io.Reader
}

func (d FloatPlainDecoder) Init(reader io.Reader) error {
	d.reader = reader

	return nil
}

func (d FloatPlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	var data uint32

	for i := range dest {
		if err := binary.Read(d.reader, binary.LittleEndian, &data); err != nil {
			return i, err
		}

		dest[i] = math.Float32frombits(data)
	}

	return len(dest), nil
}
