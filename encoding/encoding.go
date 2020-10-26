package encoding

import (
	"io"

	"github.com/hexbee-net/errors"
)

const (
	errNilWriter             = errors.Error("writer is nil")
	errNilReader             = errors.Error("reader is nil")
	errInvalidBlockSize      = errors.Error("invalid block size")
	errInvalidMiniblockCount = errors.Error("invalid mini block count")
)

type Decoder interface {
	Init(io.Reader) error
	InitSize(io.Reader) error

	Next() (int32, error)
}
