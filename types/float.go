package types //nolint:dupl // it's cleaner to keep each type separate, even with duplication

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"

	"github.com/hexbee-net/errors"
)

// Encoder /////////////////////////////

type FloatPlainEncoder struct {
	writer io.Writer
}

func (f *FloatPlainEncoder) Init(writer io.Writer) error {
	if writer == nil {
		return errors.WithStack(errNilWriter)
	}

	f.writer = writer

	return nil
}

func (f *FloatPlainEncoder) EncodeValues(values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		v, ok := values[i].(float32)
		if !ok {
			return errors.WithFields(
				errors.WithStack(errInvalidType),
				errors.Fields{
					"expected": "float32",
					"actual":   reflect.TypeOf(values[i]).String(),
				})

		}
		data[i] = math.Float32bits(v)
	}

	return binary.Write(f.writer, binary.LittleEndian, data)
}

func (f *FloatPlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type FloatPlainDecoder struct {
	reader io.Reader
}

func (d *FloatPlainDecoder) Init(reader io.Reader) error {
	if reader == nil {
		return errors.WithStack(errNilReader)
	}

	d.reader = reader

	return nil
}

func (d *FloatPlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	var data uint32

	for i := range dest {
		if err := binary.Read(d.reader, binary.LittleEndian, &data); err != nil {
			return i, errors.Wrap(err, "failed to read values data")
		}

		dest[i] = math.Float32frombits(data)
	}

	return len(dest), nil
}
