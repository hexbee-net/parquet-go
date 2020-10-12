package datastore

import (
	"hash/fnv"
	"io"

	"github.com/hexbee-net/errors"
)

var DefaultHashFunc func([]byte) interface{}

func init() {
	DefaultHashFunc = fnvHashFunc
}

func fnvHashFunc(in []byte) interface{} {
	hash := fnv.New64()
	if err := writeFull(hash, in); err != nil {
		panic(err)
	}
	return hash.Sum64()
}

func mapKey(a interface{}) interface{} {
	switch v := a.(type) {
	case int, int32, int64, string, bool, float64, float32:
		return a
	case []byte:
		return DefaultHashFunc(v)
	case [12]byte:
		return DefaultHashFunc(v[:])
	default:
		panic("not supported type")
	}
}

func writeFull(w io.Writer, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	cnt, err := w.Write(buf)
	if err != nil {
		return err
	}

	if cnt != len(buf) {
		return errors.WithFields(
			errors.New("invalid number of bytes written"),
			errors.Fields{
				"expected": cnt,
				"actual":   len(buf),
			})
	}

	return nil
}
