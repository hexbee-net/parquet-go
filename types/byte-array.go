package types

import "io"

// Encoding_PLAIN //////////////////////////////////////////////////////////////

// Encoder /////////////////////////////

// Decoder /////////////////////////////

type ByteArrayPlainDecoder struct {
	Reader io.Reader

	// if the length is set, then this is a fix size array decoder, unless it reads the len first
	Length int
}

func (d ByteArrayPlainDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d ByteArrayPlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	panic("implement me")
}

// Encoding_DELTA_LENGTH_BYTE_ARRAY ////////////////////////////////////////////

// Encoder /////////////////////////////

// Decoder /////////////////////////////

type ByteArrayDeltaLengthDecoder struct {
	r        io.Reader
	position int
	lens     []int32
}

func (d *ByteArrayDeltaLengthDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d *ByteArrayDeltaLengthDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	panic("implement me")
}

// Encoding_DELTA_BYTE_ARRAY ///////////////////////////////////////////////////

// Encoder /////////////////////////////

// Decoder /////////////////////////////

type ByteArrayDeltaDecoder struct {
	suffixDecoder ByteArrayDeltaLengthDecoder
	prefixLens    []int32
	previousValue []byte
}

func (d *ByteArrayDeltaDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d *ByteArrayDeltaDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	panic("implement me")
}
