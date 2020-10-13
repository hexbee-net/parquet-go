package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
)

type HybridEncoder struct {
	w io.Writer

	left       []int32
	original   io.Writer
	bitWidth   int
	unpackerFn pack8int32Func

	data *PackedArray
}

func NewHybridEncoder(bitWidth int) *HybridEncoder {
	return &HybridEncoder{
		bitWidth:   bitWidth,
		unpackerFn: pack8Int32FuncByWidth[bitWidth],
		data:       &PackedArray{},
	}
}

func (he *HybridEncoder) Init(writer io.Writer) error {
	he.w = writer
	he.left = nil
	he.original = nil

	he.data.Reset(he.bitWidth)

	return nil
}

func (he *HybridEncoder) InitSize(writer io.Writer) error {
	_ = he.Init(&bytes.Buffer{})
	he.original = writer

	return nil
}

func (he *HybridEncoder) Encode(data []int32) error {
	for i := range data {
		he.data.AppendSingle(data[i])
	}

	return nil
}

func (he *HybridEncoder) Write(items ...[]byte) error {
	for i := range items {
		if err := writeFull(he.w, items[i]); err != nil {
			return err
		}
	}

	return nil
}

func (he *HybridEncoder) Close() error {
	if he.bitWidth == 0 {
		return nil
	}

	if err := he.flush(); err != nil {
		return err
	}

	if he.original != nil {
		data := he.w.(*bytes.Buffer).Bytes()
		size := uint32(len(data))

		if err := binary.Write(he.original, binary.LittleEndian, size); err != nil {
			return err
		}

		return writeFull(he.original, data)
	}

	return nil
}

func (he *HybridEncoder) flush() error {
	he.data.Flush()

	return he.bpEncode()
}

func (he *HybridEncoder) bpEncode() error {
	// If the bit width is zero, no need to write any
	if he.bitWidth == 0 {
		return nil
	}

	l := he.data.count
	if x := l % 8; x != 0 {
		l += 8 - x
	}

	header := ((l / 8) << 1) | 1
	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return he.Write(buf[:cnt], he.data.data)
}
