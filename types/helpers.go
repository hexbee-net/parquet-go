package types

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
)

func encodeValue(w io.Writer, enc ValuesEncoder, all []interface{}) error {
	if err := enc.Init(w); err != nil {
		return err
	}

	if err := enc.EncodeValues(all); err != nil {
		return err
	}

	return enc.Close()
}

func decodeInt32(d encoding.Decoder, data []int32) error {
	for i := range data {
		u, err := d.Next()
		if err != nil {
			return err
		}

		data[i] = u
	}

	return nil
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

// check the b2 into b1 to find the max prefix len.
func prefix(b1, b2 []byte) int {
	l := len(b1)
	if l2 := len(b2); l > l2 {
		l = l2
	}

	for i := 0; i < l; i++ {
		if b1[i] != b2[i] {
			return i
		}
	}

	return l
}
