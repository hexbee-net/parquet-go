package datastore

import (
	"io"

	"github.com/hexbee-net/errors"
)

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
