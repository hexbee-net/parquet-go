package encoding

import "io"

type Decoder interface {
	Init(io.Reader) error
	InitSize(io.Reader) error

	Next() (int32, error)
}
