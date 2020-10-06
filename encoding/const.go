package encoding

import (
	"io"
)

type ConstDecoder int32

func (d ConstDecoder) Init(_ io.Reader) error {
	return nil
}

func (d ConstDecoder) InitSize(_ io.Reader) error {
	return nil
}

func (d ConstDecoder) Next() (int32, error) {
	return int32(d), nil
}
