package types

import "io"

type DoublePlainDecoder struct {
	Reader io.Reader
}

func (d DoublePlainDecoder) Init(reader io.Reader) error {
	panic("implement me")
}

func (d DoublePlainDecoder) DecodeValues(dest []interface{}) (int, error) {
	panic("implement me")
}
