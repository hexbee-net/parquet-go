package types

import "io"

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
