package types //nolint:dupl // it's cleaner to keep each type separate, even with duplication

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"

	"github.com/hexbee-net/errors"
)

// Encoder /////////////////////////////

type DoublePlainEncoder struct {
	writer io.Writer
}

func (e *DoublePlainEncoder) Init(writer io.Writer) error {
	if writer == nil {
		return errors.WithStack(errNilWriter)
	}

	e.writer = writer

	return nil
}

func (e *DoublePlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]uint64, len(values))

	for i := range values {
		v, ok := values[i].(float64)
		if !ok {
			return errors.WithFields(
				errors.WithStack(errInvalidType),
				errors.Fields{
					"expected": "float32",
					"actual":   reflect.TypeOf(values[i]).String(),
				})
		}

		data[i] = math.Float64bits(v)
	}

	return binary.Write(e.writer, binary.LittleEndian, data)
}

func (e *DoublePlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type DoublePlainDecoder struct {
	reader io.Reader
}

func (d *DoublePlainDecoder) Init(reader io.Reader) error {
	if reader == nil {
		return errors.WithStack(errNilReader)
	}

	d.reader = reader

	return nil
}

func (d *DoublePlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	var data uint64

	for i := range dest {
		if err := binary.Read(d.reader, binary.LittleEndian, &data); err != nil {
			return i, err
		}

		dest[i] = math.Float64frombits(data)
	}

	return len(dest), nil
}
