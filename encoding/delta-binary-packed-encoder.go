package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"

	"github.com/hexbee-net/errors"
)

// Generic encoder /////////////////////////////////////////////////////////////

const (
	blockSizeMultiple           = 128
	miniBlockValueCountMultiple = 32
)

type deltaBinaryPackEncoder struct {
	bitWidth []uint8  //nolint: structcheck // used in concrete implementations
	packed   [][]byte //nolint: structcheck // used in concrete implementations
	w        io.Writer

	// this value should be there before the init
	blockSize      int // Must be multiply of 128
	miniBlockCount int // blockSize % miniBlockCount should be 0

	miniBlockValueCount int //nolint: structcheck // used in concrete implementations

	valuesCount int
	buffer      *bytes.Buffer
	firstValue  int64

	flush           func() error
	getDeltasLength func() int
}

func (e *deltaBinaryPackEncoder) checkBlockParameters() error {
	if e.blockSize%blockSizeMultiple != 0 || e.blockSize <= 0 {
		return errors.WithFields(
			errors.WithStack(errInvalidBlockSize),
			errors.Fields{
				"block-size": e.blockSize,
			})
	}

	if e.miniBlockCount <= 0 || e.blockSize%e.miniBlockCount != 0 {
		return errors.WithFields(
			errors.WithStack(errInvalidMiniblockCount),
			errors.Fields{
				"block-count": e.miniBlockCount,
			})
	}

	e.miniBlockValueCount = e.blockSize / e.miniBlockCount
	if e.miniBlockValueCount%miniBlockValueCountMultiple != 0 {
		return errors.WithFields(
			errors.Wrapf(errInvalidMiniblockCount, "the quotient of the block size and the miniblocks count should be a multiple of %d", miniBlockValueCountMultiple),
			errors.Fields{
				"block-count": e.miniBlockCount,
			})
	}

	return nil
}

func (e *deltaBinaryPackEncoder) write() error {
	if e.getDeltasLength() > 0 {
		if err := e.flush(); err != nil {
			return err
		}
	}

	if err := writeUVarInt64(e.w, uint64(e.blockSize)); err != nil {
		return err
	}

	if err := writeUVarInt64(e.w, uint64(e.miniBlockCount)); err != nil {
		return err
	}

	if err := writeUVarInt64(e.w, uint64(e.valuesCount)); err != nil {
		return err
	}

	if err := writeVarInt64(e.w, e.firstValue); err != nil {
		return err
	}

	return writeFull(e.w, e.buffer.Bytes())
}

// Int32 ///////////////////////////////////////////////////////////////////////

type DeltaBinaryPackEncoder32 struct {
	deltaBinaryPackEncoder

	deltas []int32

	minDelta      int32
	previousValue int32
}

func NewDeltaBinaryPackEncoder32(blockSize, miniblockCount int) DeltaBinaryPackEncoder32 {
	e := DeltaBinaryPackEncoder32{
		deltaBinaryPackEncoder: deltaBinaryPackEncoder{
			blockSize:      blockSize,
			miniBlockCount: miniblockCount,
		}}

	return e
}

func (e *DeltaBinaryPackEncoder32) Init(writer io.Writer) error {
	if writer == nil {
		return errors.WithStack(errNilWriter)
	}

	e.getDeltasLength = e.getDeltasLength32
	e.flush = e.flush32

	e.w = writer

	if err := e.checkBlockParameters(); err != nil {
		return err
	}

	e.firstValue = 0
	e.valuesCount = 0
	e.minDelta = math.MaxInt32
	e.deltas = make([]int32, 0, e.blockSize)
	e.previousValue = 0
	e.buffer = &bytes.Buffer{}
	e.bitWidth = make([]uint8, 0, e.miniBlockCount)

	return nil
}

func (e *DeltaBinaryPackEncoder32) AddInt32(n int32) error {
	e.valuesCount++
	if e.valuesCount == 1 {
		e.firstValue = int64(n)
		e.previousValue = n

		return nil
	}

	delta := n - e.previousValue
	e.previousValue = n
	e.deltas = append(e.deltas, delta)

	if delta < e.minDelta {
		e.minDelta = delta
	}

	if len(e.deltas) == e.blockSize {
		return e.flush()
	}

	return nil
}

func (e *DeltaBinaryPackEncoder32) Close() error {
	return e.write()
}

func (e *DeltaBinaryPackEncoder32) getDeltasLength32() int {
	return len(e.deltas)
}

func (e *DeltaBinaryPackEncoder32) flush32() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range e.deltas {
		e.deltas[i] -= e.minDelta
	}

	if err := writeVarInt64(e.buffer, int64(e.minDelta)); err != nil {
		return err
	}

	e.bitWidth = e.bitWidth[:0] // reset the bitWidth buffer
	e.packed = e.packed[:0]

	for i := 0; i < len(e.deltas); i += e.miniBlockValueCount { //nolint:dupl // the code is duplicated for int32 and int64
		const bufSize = 8

		end := i + e.miniBlockValueCount
		if end >= len(e.deltas) {
			end = len(e.deltas)
		}

		max := uint32(e.deltas[i])
		buf := make([][bufSize]int32, e.miniBlockValueCount/bufSize)

		for j := i; j < end; j++ {
			if max < uint32(e.deltas[j]) {
				max = uint32(e.deltas[j])
			}

			t := j - i
			buf[t/bufSize][t%bufSize] = e.deltas[j]
		}

		bw := bits.Len32(max)
		e.bitWidth = append(e.bitWidth, uint8(bw))

		data := make([]byte, 0, bw*len(buf))
		pack := pack8Int32FuncByWidth[bw]

		for j := range buf {
			data = append(data, pack(buf[j])...)
		}

		e.packed = append(e.packed, data)
	}

	for len(e.bitWidth) < e.miniBlockCount {
		e.bitWidth = append(e.bitWidth, 0)
	}

	if err := binary.Write(e.buffer, binary.LittleEndian, e.bitWidth); err != nil {
		return err
	}

	for i := range e.packed {
		if err := writeFull(e.buffer, e.packed[i]); err != nil {
			return err
		}
	}

	e.minDelta = math.MaxInt32
	e.deltas = e.deltas[:0]

	return nil
}

// Int64 ///////////////////////////////////////////////////////////////////////

type DeltaBinaryPackEncoder64 struct {
	deltaBinaryPackEncoder

	deltas []int64

	minDelta      int64
	previousValue int64
}

func NewDeltaBinaryPackEncoder64(blockSize, miniBlockCount int) DeltaBinaryPackEncoder64 {
	e := DeltaBinaryPackEncoder64{
		deltaBinaryPackEncoder: deltaBinaryPackEncoder{
			blockSize:      blockSize,
			miniBlockCount: miniBlockCount,
		}}

	return e
}

func (e *DeltaBinaryPackEncoder64) Init(writer io.Writer) error {
	if writer == nil {
		return errors.WithStack(errNilWriter)
	}

	e.getDeltasLength = e.getDeltasLength64
	e.flush = e.flush64

	e.w = writer

	if err := e.checkBlockParameters(); err != nil {
		return err
	}

	e.firstValue = 0
	e.valuesCount = 0
	e.minDelta = math.MaxInt32
	e.deltas = make([]int64, 0, e.blockSize)
	e.previousValue = 0
	e.buffer = &bytes.Buffer{}
	e.bitWidth = make([]uint8, 0, e.miniBlockCount)

	return nil
}

func (e *DeltaBinaryPackEncoder64) Close() error {
	return e.write()
}

func (e *DeltaBinaryPackEncoder64) AddInt64(n int64) error {
	e.valuesCount++
	if e.valuesCount == 1 {
		e.firstValue = n
		e.previousValue = n

		return nil
	}

	delta := n - e.previousValue
	e.previousValue = n
	e.deltas = append(e.deltas, delta)

	if delta < e.minDelta {
		e.minDelta = delta
	}

	if len(e.deltas) == e.blockSize {
		return e.flush()
	}

	return nil
}

func (e *DeltaBinaryPackEncoder64) getDeltasLength64() int {
	return len(e.deltas)
}

func (e *DeltaBinaryPackEncoder64) flush64() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range e.deltas {
		e.deltas[i] -= e.minDelta
	}

	if err := writeVarInt64(e.buffer, e.minDelta); err != nil {
		return err
	}

	e.bitWidth = e.bitWidth[:0] // reset the bitWidth buffer
	e.packed = e.packed[:0]

	for i := 0; i < len(e.deltas); i += e.miniBlockValueCount { //nolint:dupl // the code is duplicated for int43 and int64
		const bufSize = 8

		end := i + e.miniBlockValueCount
		if end >= len(e.deltas) {
			end = len(e.deltas)
		}

		max := uint64(e.deltas[i])
		buf := make([][bufSize]int64, e.miniBlockValueCount/bufSize)

		for j := i; j < end; j++ {
			if max < uint64(e.deltas[j]) {
				max = uint64(e.deltas[j])
			}

			t := j - i
			buf[t/bufSize][t%bufSize] = e.deltas[j]
		}

		bw := bits.Len64(max)
		e.bitWidth = append(e.bitWidth, uint8(bw))

		data := make([]byte, 0, bw*len(buf))
		packer := pack8Int64FuncByWidth[bw]

		for j := range buf {
			data = append(data, packer(buf[j])...)
		}

		e.packed = append(e.packed, data)
	}

	for len(e.bitWidth) < e.miniBlockCount {
		e.bitWidth = append(e.bitWidth, 0)
	}

	if err := binary.Write(e.buffer, binary.LittleEndian, e.bitWidth); err != nil {
		return err
	}

	for i := range e.packed {
		if err := writeFull(e.buffer, e.packed[i]); err != nil {
			return err
		}
	}

	e.minDelta = math.MaxInt32
	e.deltas = e.deltas[:0]

	return nil
}
