package encoding

import (
	"io"

	"github.com/hexbee-net/errors"
)

const packedArrayBufSize = 8

// PackedArray is a bitmap encoded array mainly for repetition and definition
// levels, which normally have low values (~<10), a []uint16 array is not the
// most memory efficient structure due to the large number of values. Memory
// storage requirements for the packed array are ~1/8th compared to
// []uint16 array.
type PackedArray struct {
	count int
	bw    int
	data  []byte

	buf    [packedArrayBufSize]int32
	bufPos int

	writer pack8int32Func
	reader unpack8int32Func
}

func (a *PackedArray) Reset(bitWidth int) {
	if bitWidth < 0 || bitWidth > 32 {
		panic("invalid bit width")
	}

	a.bw = bitWidth
	a.count = 0
	a.bufPos = 0
	a.data = a.data[:0]
	a.writer = pack8Int32FuncByWidth[bitWidth]
	a.reader = unpack8Int32FuncByWidth[bitWidth]
}

func (a *PackedArray) Flush() {
	for i := a.bufPos; i < 8; i++ {
		a.buf[i] = 0
	}

	a.data = append(a.data, a.writer(a.buf)...)
	a.bufPos = 0
}

func (a *PackedArray) AppendSingle(v int32) {
	if a.bufPos == packedArrayBufSize {
		a.Flush()
	}

	a.buf[a.bufPos] = v
	a.bufPos++
	a.count++
}

func (a *PackedArray) AppendArray(other *PackedArray) error {
	if a.bw != other.bw {
		return errors.WithFields(
			errors.New("cannot append array with different bit width"),
			errors.Fields{
				"this":  a.bw,
				"other": other.bw,
			})
	}

	if cap(a.data) < len(a.data)+len(other.data)+1 {
		data := make([]byte, len(a.data), len(a.data)+len(other.data)+1)
		copy(data, a.data)
		a.data = data
	}

	for i := 0; i < other.count; i++ {
		v, err := other.at(i)
		if err != nil {
			return err
		}

		a.AppendSingle(v)
	}

	return nil
}

func (a *PackedArray) Write(writer io.Writer) error {
	return writeFull(writer, a.data)
}

func (a *PackedArray) at(pos int) (int32, error) {
	if pos < 0 || pos >= a.count {
		return 0, errors.New("out of range")
	}

	if a.bw == 0 {
		return 0, nil
	}

	block := (pos / 8) * a.bw
	idx := pos % 8

	if block >= len(a.data) {
		return a.buf[idx], nil
	}

	buf := a.reader(a.data[block : block+a.bw])

	return buf[idx], nil
}
