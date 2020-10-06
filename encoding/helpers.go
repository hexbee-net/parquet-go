package encoding

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/hexbee-net/errors"
)

type byteReader struct {
	io.Reader
}

func (r byteReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r.Reader, buf); err != nil {
		return 0, err
	}

	return buf[0], nil
}

func readUVariant32(r io.Reader) (int32, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	i, err := binary.ReadUvarint(b)
	if err != nil {
		return 0, err
	}

	if i > math.MaxInt32 {
		return 0, errors.New("int32 out of range")
	}

	return int32(i), nil
}

func readVariant32(r io.Reader) (int32, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	i, err := binary.ReadVarint(b)
	if err != nil {
		return 0, err
	}

	if i > math.MaxInt32 || i < math.MinInt32 {
		return 0, errors.New("int32 out of range")
	}

	return int32(i), nil
}

func readVariant64(r io.Reader) (int64, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	return binary.ReadVarint(b)
}

func writeVariant(w io.Writer, in int64) error {
	buf := make([]byte, 12)
	n := binary.PutVarint(buf, in)

	return writeFull(w, buf[:n])
}

func writeUVariant(w io.Writer, in uint64) error {
	buf := make([]byte, 12)
	n := binary.PutUvarint(buf, in)

	return writeFull(w, buf[:n])
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
