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

func readUVarInt32(r io.Reader) (int32, error) {
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

func readVarInt32(r io.Reader) (int32, error) {
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

func readVarUInt64(r io.Reader) (uint64, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	i, err := binary.ReadUvarint(b)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func readVarInt64(r io.Reader) (int64, error) {
	b, ok := r.(io.ByteReader)
	if !ok {
		b = &byteReader{Reader: r}
	}

	return binary.ReadVarint(b)
}

func writeVarInt64(w io.Writer, in int64) error {
	buf := make([]byte, 12)
	n := binary.PutVarint(buf, in)

	return writeFull(w, buf[:n])
}

func writeUVarInt64(w io.Writer, in uint64) error {
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

func writeIntLittleEndianOnOneByte(w io.Writer, in int64) error {
	b := []byte{
		byte(in),
	}

	return writeFull(w, b)
}

func writeIntLittleEndianOnTwoByte(w io.Writer, in int64) error {
	b := []byte{
		byte(in),
		byte(in >> 8),
	}

	return writeFull(w, b)
}

func writeIntLittleEndianOnThreeByte(w io.Writer, in int64) error {
	b := []byte{
		byte(in),
		byte(in >> 8),
		byte(in >> 16),
	}

	return writeFull(w, b)
}

func writeIntLittleEndianOnFourByte(w io.Writer, in int64) error {
	b := []byte{
		byte(in),
		byte(in >> 8),
		byte(in >> 16),
		byte(in >> 24),
	}

	return writeFull(w, b)
}

func readIntLittleEndianOnOneByte(r io.Reader) (int64, error) {
	b := make([]byte, 1)

	if _, err := r.Read(b); err != nil {
		return 0, err
	}

	v := int64(b[0])
	if v < 0 {
		return 0, io.EOF
	}

	return v, nil
}

func readIntLittleEndianOnTwoByte(r io.Reader) (int64, error) {
	b := make([]byte, 2)

	if _, err := r.Read(b); err != nil {
		return 0, err
	}

	v1 := int64(b[0])
	v2 := int64(b[1])
	if (v1 | v2) < 0 {
		return 0, io.EOF
	}

	return (v2 << 8) + v1, nil
}

func readIntLittleEndianOnThreeByte(r io.Reader) (int64, error) {
	b := make([]byte, 3)

	if _, err := r.Read(b); err != nil {
		return 0, err
	}

	v1 := int64(b[0])
	v2 := int64(b[1])
	v3 := int64(b[2])
	if (v1 | v2 | v3) < 0 {
		return 0, io.EOF
	}

	return (v3 << 16) + (v2 << 8) + v1, nil
}

func readIntLittleEndianOnFourByte(r io.Reader) (int64, error) {
	b := make([]byte, 4)

	if _, err := r.Read(b); err != nil {
		return 0, err
	}

	v1 := int64(b[0])
	v2 := int64(b[1])
	v3 := int64(b[2])
	v4 := int64(b[3])
	if (v1 | v2 | v3 | v4) < 0 {
		return 0, io.EOF
	}

	return (v4 << 24) + (v3 << 16) + (v2 << 8) + v1, nil
}
