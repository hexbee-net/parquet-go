package types

import (
	"io"

	"github.com/hexbee-net/parquet/encoding"
)

// Encoding_PLAIN //////////////////////////////////////////////////////////////

// Encoder /////////////////////////////

type BooleanPlainEncoder struct {
	writer io.Writer
	data   *encoding.PackedArray
}

func (e *BooleanPlainEncoder) Init(writer io.Writer) error {
	e.writer = writer

	e.data = &encoding.PackedArray{}
	e.data.Reset(1)

	return nil
}

func (e *BooleanPlainEncoder) EncodeValues(values []interface{}) error {
	for i := range values {
		var v int32
		if values[i].(bool) {
			v = 1
		}

		e.data.AppendSingle(v)
	}

	return nil
}

func (e *BooleanPlainEncoder) Close() error {
	e.data.Flush()
	return e.data.Write(e.writer)
}

// Decoder /////////////////////////////

type BooleanPlainDecoder struct {
	reader io.Reader
	left   []bool
}

func (d *BooleanPlainDecoder) Init(reader io.Reader) error {
	d.reader = reader
	d.left = nil

	return nil
}

func (d *BooleanPlainDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	start := 0

	if len(d.left) > 0 {
		// there is a leftover from the last run
		d.left, start = copyLeftOvers(dest, d.left)

		if d.left != nil {
			return len(dest), nil
		}
	}

	buf := make([]byte, 1)

	for i := start; i < len(dest); i += 8 {
		if _, err := io.ReadFull(d.reader, buf); err != nil {
			return i, err
		}

		b := unpackByte(buf)

		for j := 0; j < 8; j++ {
			if i+j < len(dest) {
				dest[i+j] = b[j] == 1
			} else {
				d.left = append(d.left, b[j] == 1)
			}
		}
	}

	return len(dest), nil
}

func unpackByte(data []byte) (a [8]int32) {
	_ = data[0]
	a[0] = int32((data[0] >> 0) & 1)
	a[1] = int32((data[0] >> 1) & 1)
	a[2] = int32((data[0] >> 2) & 1)
	a[3] = int32((data[0] >> 3) & 1)
	a[4] = int32((data[0] >> 4) & 1)
	a[5] = int32((data[0] >> 5) & 1)
	a[6] = int32((data[0] >> 6) & 1)
	a[7] = int32((data[0] >> 7) & 1)

	return a
}

// copy the left overs from the previous call. instead of returning an empty subset of the old slice,
// it delete the slice (by returning nil) so there is no memory leak because of the underlying array
// the return value is the new left over and the number of read message.
func copyLeftOvers(dest []interface{}, src []bool) (leftOver []bool, readCount int) {
	size := len(dest)
	clean := false

	if len(src) <= size {
		size = len(src)
		clean = true
	}

	for i := 0; i < size; i++ {
		dest[i] = src[i]
	}

	if clean {
		return nil, size
	}

	return src[size:], size
}

// Encoding_RLE_DICTIONARY /////////////////////////////////////////////////////

// Encoder /////////////////////////////

type BooleanRLEEncoder struct {
	encoder *encoding.HybridEncoder
}

func (e *BooleanRLEEncoder) Init(writer io.Writer) error {
	e.encoder = encoding.NewHybridEncoder(1)

	return e.encoder.InitSize(writer)
}

func (e *BooleanRLEEncoder) EncodeValues(values []interface{}) error {
	buf := make([]int32, len(values))

	for i := range values {
		if values[i].(bool) {
			buf[i] = 1
		} else {
			buf[i] = 0
		}
	}

	return e.encoder.Encode(buf)
}

func (e *BooleanRLEEncoder) Close() error {
	return e.encoder.Close()
}

// Decoder /////////////////////////////

type BooleanRLEDecoder struct {
	decoder *encoding.HybridDecoder
}

func (d *BooleanRLEDecoder) Init(reader io.Reader) error {
	d.decoder = encoding.NewHybridDecoder(1, false)

	return d.decoder.InitSize(reader)
}

func (d *BooleanRLEDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	total := len(dest)

	for i := 0; i < total; i++ {
		n, err := d.decoder.Next()
		if err != nil {
			return i, err
		}

		dest[i] = n == 1
	}

	return total, nil
}
