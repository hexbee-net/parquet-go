package types //nolint:dupl // it's cleaner to keep each type separate, even with duplication

import (
	"encoding/binary"
	"io"

	"github.com/hexbee-net/parquet/encoding"
)

// Encoding_PLAIN //////////////////////////////////////////////////////////////

// Encoder /////////////////////////////

type Int32PlainEncoder struct {
	writer   io.Writer
	Unsigned bool
}

func (e *Int32PlainEncoder) Init(writer io.Writer) error {
	e.writer = writer
	return nil
}

func (e *Int32PlainEncoder) EncodeValues(values []interface{}) error {
	d := make([]int32, len(values))

	if e.Unsigned {
		for i := range values {
			d[i] = int32(values[i].(uint32))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int32)
		}
	}

	return binary.Write(e.writer, binary.LittleEndian, d)
}

func (e *Int32PlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type Int32PlainDecoder struct {
	reader   io.Reader
	Unsigned bool
}

func (d *Int32PlainDecoder) Init(reader io.Reader) error {
	d.reader = reader
	return nil
}

func (d *Int32PlainDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	var n int32

	for count = range dest {
		if err := binary.Read(d.reader, binary.LittleEndian, &n); err != nil {
			return count, err
		}

		if d.Unsigned {
			dest[count] = uint32(n)
		} else {
			dest[count] = n
		}
	}

	return len(dest), nil
}

// Encoding_DELTA_BINARY_PACKED ////////////////////////////////////////////////

// Encoder /////////////////////////////

type Int32DeltaBPEncoder struct {
	encoding.DeltaBinaryPackEncoder32
	Unsigned bool
}

func (e *Int32DeltaBPEncoder) EncodeValues(values []interface{}) error {
	if e.Unsigned {
		for i := range values {
			if err := e.AddInt32(int32(values[i].(uint32))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := e.AddInt32(values[i].(int32)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Decoder /////////////////////////////

type Int32DeltaBPDecoder struct {
	encoding.DeltaBinaryPackDecoder32
	Unsigned bool
}

func (d *Int32DeltaBPDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	for i := range dest {
		u, err := d.Next()
		if err != nil {
			return i, err
		}

		if d.Unsigned {
			dest[i] = uint32(u)
		} else {
			dest[i] = u
		}
	}

	return len(dest), nil
}
