package types

import (
	"encoding/binary"
	"io"

	"github.com/hexbee-net/parquet/encoding"
)

// Encoding_PLAIN //////////////////////////////////////////////////////////////

// Encoder /////////////////////////////

type Int64PlainEncoder struct {
	Writer   io.Writer
	Unsigned bool
}

func (e *Int64PlainEncoder) Init(writer io.Writer) error {
	e.Writer = writer
	return nil
}

func (e *Int64PlainEncoder) EncodeValues(values []interface{}) error {
	d := make([]int64, len(values))

	if e.Unsigned {
		for i := range values {
			d[i] = int64(values[i].(uint32))
		}
	} else {
		for i := range values {
			d[i] = values[i].(int64)
		}
	}

	return binary.Write(e.Writer, binary.LittleEndian, d)
}

func (e *Int64PlainEncoder) Close() error {
	return nil
}

// Decoder /////////////////////////////

type Int64PlainDecoder struct {
	Reader   io.Reader
	Unsigned bool
}

func (d *Int64PlainDecoder) Init(reader io.Reader) error {
	d.Reader = reader
	return nil
}

func (d *Int64PlainDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	var n int64

	for count = range dest {
		if err := binary.Read(d.Reader, binary.LittleEndian, &n); err != nil {
			return count, err
		}

		if d.Unsigned {
			dest[count] = uint64(n)
		} else {
			dest[count] = n
		}
	}

	return len(dest), nil
}

// Encoding_DELTA_BINARY_PACKED ////////////////////////////////////////////////

// Encoder /////////////////////////////

type Int64DeltaBPEncoder struct {
	encoding.DeltaBinaryPackEncoder64
	Unsigned bool
}

func (e *Int64DeltaBPEncoder) EncodeValues(values []interface{}) error {
	if e.Unsigned {
		for i := range values {
			if err := e.AddInt64(int64(values[i].(uint64))); err != nil {
				return err
			}
		}
	} else {
		for i := range values {
			if err := e.AddInt64(values[i].(int64)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Decoder /////////////////////////////

type Int64DeltaBPDecoder struct {
	encoding.DeltaBinaryPackDecoder64
	Unsigned bool
}

func (d *Int64DeltaBPDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	for i := range dest {
		u, err := d.Next()
		if err != nil {
			return i, err
		}

		if d.Unsigned {
			dest[i] = uint64(u)
		} else {
			dest[i] = u
		}
	}

	return len(dest), nil
}
