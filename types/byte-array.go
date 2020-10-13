package types

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
)

const (
	deltaLengthBlockSize     = 128
	deltaBinaryPackBlockSize = 128
)

// Encoding_PLAIN //////////////////////////////////////////////////////////////

// Encoder /////////////////////////////

type ByteArrayPlainEncoder struct {
	writer io.Writer

	Length int
}

func (e ByteArrayPlainEncoder) Init(writer io.Writer) error {
	e.writer = writer

	return nil
}

func (e ByteArrayPlainEncoder) EncodeValues(values []interface{}) error {
	for i := range values {
		if err := e.writeBytes(values[i].([]byte)); err != nil {
			return err
		}
	}

	return nil
}

func (e ByteArrayPlainEncoder) Close() error {
	return nil
}

func (e ByteArrayPlainEncoder) writeBytes(data []byte) error {
	l := e.Length

	if l == 0 { // variable length
		l = len(data)
		l32 := int32(l)

		if err := binary.Write(e.writer, binary.LittleEndian, l32); err != nil {
			return err
		}
	} else if len(data) != l {
		return errors.WithFields(
			errors.New("byte array has invalid length"),
			errors.Fields{
				"expected": l,
				"actual":   len(data),
			})
	}

	return writeFull(e.writer, data)
}

// Decoder /////////////////////////////

type ByteArrayPlainDecoder struct {
	reader io.Reader

	// if the length is set, then this is a fix size array decoder, unless it reads the len first
	Length int
}

func (d ByteArrayPlainDecoder) Init(reader io.Reader) error {
	d.reader = reader

	return nil
}

func (d ByteArrayPlainDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	for i := range dest {
		if dest[i], err = d.next(); err != nil {
			return i, err
		}
	}

	return len(dest), nil
}

func (d ByteArrayPlainDecoder) next() ([]byte, error) {
	var l = int32(d.Length)
	if l == 0 {
		if err := binary.Read(d.reader, binary.LittleEndian, &l); err != nil {
			return nil, err
		}

		if l < 0 {
			return nil, errors.New("bytearray/plain: len is negative")
		}
	}

	buf := make([]byte, l)

	_, err := io.ReadFull(d.reader, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// Encoding_DELTA_LENGTH_BYTE_ARRAY ////////////////////////////////////////////

// Encoder /////////////////////////////

type ByteArrayDeltaLengthEncoder struct {
	writer io.Writer
	buf    *bytes.Buffer
	lens   []interface{}
}

func (e ByteArrayDeltaLengthEncoder) Init(writer io.Writer) error {
	e.writer = writer
	e.buf = &bytes.Buffer{}

	return nil
}

func (e ByteArrayDeltaLengthEncoder) EncodeValues(values []interface{}) error {
	if e.lens == nil {
		// this is just for the first time, maybe we need to copy and increase the cap in the next calls?
		e.lens = make([]interface{}, 0, len(values))
	}

	for i := range values {
		if err := e.writeOne(values[i].([]byte)); err != nil {
			return err
		}
	}

	return nil
}

func (e ByteArrayDeltaLengthEncoder) Close() error {
	enc := &Int32DeltaBPEncoder{
		DeltaBinaryPackEncoder32: encoding.NewDeltaBinaryPackEncoder32(deltaLengthBlockSize, 4),
	}

	if err := encodeValue(e.writer, enc, e.lens); err != nil {
		return err
	}

	return writeFull(e.writer, e.buf.Bytes())
}

func (e ByteArrayDeltaLengthEncoder) writeOne(data []byte) error {
	e.lens = append(e.lens, int32(len(data)))

	return writeFull(e.buf, data)
}

// Decoder /////////////////////////////

type ByteArrayDeltaLengthDecoder struct {
	reader   io.Reader
	position int
	lens     []int32
}

func (d *ByteArrayDeltaLengthDecoder) Init(reader io.Reader) error {
	d.reader = reader
	d.position = 0
	lensDecoder := Int32DeltaBPDecoder{}

	if err := lensDecoder.Init(reader); err != nil {
		return err
	}

	d.lens = make([]int32, lensDecoder.ValuesCount)

	return decodeInt32(&lensDecoder, d.lens)
}

func (d *ByteArrayDeltaLengthDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	total := len(dest)

	for i := 0; i < total; i++ {
		v, err := d.next()

		if err != nil {
			return i, err
		}

		dest[i] = v
	}

	return total, nil
}

func (d *ByteArrayDeltaLengthDecoder) next() ([]byte, error) {
	if d.position >= len(d.lens) {
		return nil, io.EOF
	}

	size := int(d.lens[d.position])
	value := make([]byte, size)

	if _, err := io.ReadFull(d.reader, value); err != nil {
		return nil, errors.Wrap(err, "there is no byte left")
	}

	d.position++

	return value, nil
}

// Encoding_DELTA_BYTE_ARRAY ///////////////////////////////////////////////////

// Encoder /////////////////////////////

type ByteArrayDeltaEncoder struct {
	writer io.Writer

	prefixLens    []interface{}
	previousValue []byte

	values *ByteArrayDeltaLengthEncoder
}

func (b ByteArrayDeltaEncoder) Init(writer io.Writer) error {
	b.writer = writer
	b.prefixLens = nil
	b.previousValue = []byte{}
	b.values = &ByteArrayDeltaLengthEncoder{}

	return b.values.Init(writer)
}

func (b ByteArrayDeltaEncoder) EncodeValues(values []interface{}) error {
	if b.prefixLens == nil {
		b.prefixLens = make([]interface{}, 0, len(values))
		b.values.lens = make([]interface{}, 0, len(values))
	}

	for i := range values {
		data := values[i].([]byte)
		pLen := prefix(b.previousValue, data)
		b.prefixLens = append(b.prefixLens, int32(pLen))

		if err := b.values.writeOne(data[pLen:]); err != nil {
			return err
		}

		b.previousValue = data
	}

	return nil
}

func (b ByteArrayDeltaEncoder) Close() error {
	// write the lens first
	enc := &Int32DeltaBPEncoder{
		DeltaBinaryPackEncoder32: encoding.NewDeltaBinaryPackEncoder32(deltaBinaryPackBlockSize, 4),
	}

	if err := encodeValue(b.writer, enc, b.prefixLens); err != nil {
		return err
	}

	return b.values.Close()
}

// Decoder /////////////////////////////

type ByteArrayDeltaDecoder struct {
	suffixDecoder ByteArrayDeltaLengthDecoder
	prefixLens    []int32
	previousValue []byte
}

func (d *ByteArrayDeltaDecoder) Init(reader io.Reader) error {
	lensDecoder := encoding.DeltaBinaryPackDecoder32{}

	if err := lensDecoder.Init(reader); err != nil {
		return err
	}

	d.prefixLens = make([]int32, lensDecoder.ValuesCount)
	if err := decodeInt32(&lensDecoder, d.prefixLens); err != nil {
		return err
	}

	if err := d.suffixDecoder.Init(reader); err != nil {
		return err
	}

	if len(d.prefixLens) != len(d.suffixDecoder.lens) {
		return errors.WithFields(
			errors.New("bytearray/delta: different number of suffixes and prefixes"),
			errors.Fields{
				"prefix": len(d.prefixLens),
				"suffix": len(d.suffixDecoder.lens),
			})
	}

	d.previousValue = make([]byte, 0)

	return nil
}

func (d *ByteArrayDeltaDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	total := len(dest)

	for i := 0; i < total; i++ {
		suffix, err := d.suffixDecoder.next()
		if err != nil {
			return i, err
		}

		// after this line no error is acceptable
		prefixLen := int(d.prefixLens[d.suffixDecoder.position-1])
		value := make([]byte, 0, prefixLen+len(suffix))

		if len(d.previousValue) < prefixLen {
			// prevent panic from invalid input
			return 0, errors.WithFields(
				errors.New("invalid prefix len in the stream"),
				errors.Fields{
					"expected": prefixLen,
					"actual":   len(d.previousValue),
				})
		}

		if prefixLen > 0 {
			value = append(value, d.previousValue[:prefixLen]...)
		}

		value = append(value, suffix...)
		d.previousValue = value
		dest[i] = value
	}

	return total, nil
}
