package types

import (
	"encoding/binary"
	"io"
	"math"
)

// Encoder /////////////////////////////

type DoublePlainEncoder struct {
	writer io.Writer
}

func (e DoublePlainEncoder) Init(writer io.Writer) error {
	e.writer = writer

	return nil
}

func (e DoublePlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(e.writer, binary.LittleEndian, data)
}

func (e DoublePlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type DoublePlainDecoder struct {
	reader io.Reader
}

func (d DoublePlainDecoder) Init(reader io.Reader) error {
	d.reader = reader

	return nil
}

func (d DoublePlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	var data uint64
	for i := range dest {
		if err := binary.Read(d.reader, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dest[i] = math.Float64frombits(data)
	}

	return len(dest), nil
}
