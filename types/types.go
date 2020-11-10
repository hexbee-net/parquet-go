package types

import (
	"io"

	"github.com/hexbee-net/errors"
)

const (
	errInvalidType = errors.Error("invalid type")
	errNilWriter   = errors.Error("writer is nil")
	errNilReader   = errors.Error("reader is nil")
)

type ValuesEncoder interface {
	io.Closer

	Init(io.Writer) error
	EncodeValues(values []interface{}) error
}

type ValuesDecoder interface {
	Init(io.Reader) error

	// the error io.EOF with the less value is acceptable, any other error is not
	DecodeValues(dest []interface{}) (count int, err error)
}

type DictValuesEncoder interface {
	ValuesEncoder

	getValues() []interface{}
}

type DictValuesDecoder interface {
	ValuesDecoder

	setValues([]interface{})
}
