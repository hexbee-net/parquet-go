package types

import "io"

type FloatPlainDecoder struct {
	Reader io.Reader
}

func (d FloatPlainDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d FloatPlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	panic("implement me")
}
