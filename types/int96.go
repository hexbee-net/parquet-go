package types

import "io"

type Int96PlainDecoder struct {
	Reader io.Reader
}

func (d Int96PlainDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d Int96PlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	panic("implement me")
}
