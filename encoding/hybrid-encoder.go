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

func (e *HybridEncoder) Init(writer io.Writer) error {
	e.w = writer
	e.left = nil
	e.original = nil

	if err := e.data.Reset(e.bitWidth); err != nil {
		return err
	}

	return nil
}

func (e *HybridEncoder) InitSize(writer io.Writer) error {
	if err := e.Init(&bytes.Buffer{}); err != nil {
		return err
	}

	e.original = writer

	return nil
}

func (e *HybridEncoder) Encode(data []int32) error {
	for i := range data {
		e.data.AppendSingle(data[i])
	}

	return nil
}

func (e *HybridEncoder) Write(items ...[]byte) error {
	for i := range items {
		if err := writeFull(e.w, items[i]); err != nil {
			return err
		}
	}

	return nil
}

func (e *HybridEncoder) Close() error {
	if e.bitWidth == 0 {
		return nil
	}

	if err := e.flush(); err != nil {
		return err
	}

	if e.original != nil {
		data := e.w.(*bytes.Buffer).Bytes()
		size := uint32(len(data))

		if err := binary.Write(e.original, binary.LittleEndian, size); err != nil {
			return err
		}

		return writeFull(e.original, data)
	}

	return nil
}

func (e *HybridEncoder) flush() error {
	e.data.Flush()

	return e.bpEncode()
}

func (e *HybridEncoder) bpEncode() error {
	// If the bit width is zero, no need to write any
	if e.bitWidth == 0 {
		return nil
	}

	l := e.data.count
	if x := l % 8; x != 0 {
		l += 8 - x
	}

	header := ((l / 8) << 1) | 1
	buf := make([]byte, 4) // big enough for int
	cnt := binary.PutUvarint(buf, uint64(header))

	return e.Write(buf[:cnt], e.data.data)
}
