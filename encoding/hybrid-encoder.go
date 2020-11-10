package encoding

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/hexbee-net/errors"
)

const (
	rleBufSize           = 8
	rleThreshold         = 8
	rleMaxBitPackedCount = 63
)

type HybridEncoder struct {
	bitWidth            int    // The bit width used for bit-packing and for writing the repeated-value.
	previousValue       int32  // Previous value written, used to detect repeated values.
	repeatCount         int    // How many times a value has been repeated.
	bitPackedGroupCount int    // How many groups of 8 values have been written to the current bit-packed-run.
	packBuffer          []byte // Values that are bit packed 8 at at a time are packed into this buffer, which is then written to encodingBuffer

	packData []byte

	// We buffer values and either bit pack them, or discard them after writing a rle-run.
	valuesBuffer    [rleBufSize]int32
	valuesBufferPos int

	encodingBuffer bytes.Buffer

	// A "pointer" to a single byte in encodingBuffer, which we use as our bit-packed-header.
	// It's really the logical index of the byte in encodingBuffer.
	//
	// We are only using one byte for this header, which limits us to
	// writing 504 values per bit-packed-run.
	//
	// MSB must be 0 for varint encoding, LSB must be 1 to signify
	// that this is a bit-packed-header leaves 6 bits to write the
	// number of 8-groups -> (2^6 - 1) * 8 = 504
	bitPackedRunHeaderPointer *int
	bitPackingRun             bool

	packValues pack8int32Func
}

func NewHybridEncoder(bitWidth int) (*HybridEncoder, error) {
	if bitWidth < 0 || bitWidth > 32 {
		return nil, errors.WithFields(
			errors.WithStack(errInvalidBitWidth),
			errors.Fields{
				"bit-width": bitWidth,
			})
	}

	e := &HybridEncoder{
		bitWidth:   bitWidth,
		packBuffer: make([]byte, 0),
		packValues: pack8Int32FuncByWidth[bitWidth],
	}

	return e, nil
}

func (e *HybridEncoder) Reset(bitWidth int) error {
	if bitWidth < 0 || bitWidth > 32 {
		return errors.WithFields(
			errors.WithStack(errInvalidBitWidth),
			errors.Fields{
				"bit-width": bitWidth,
			})
	}

	e.encodingBuffer.Reset()

	e.packBuffer = e.packBuffer[:0]
	e.bitWidth = bitWidth
	e.previousValue = 0
	e.repeatCount = 0
	e.bitPackedGroupCount = 0
	e.valuesBufferPos = 0
	e.bitPackedRunHeaderPointer = nil
	e.packValues = pack8Int32FuncByWidth[bitWidth]

	return nil
}

func (e *HybridEncoder) AppendSingle(v int32) error {
	if v == e.previousValue { //nolint:nestif // the alternative to nested ifs is actually less readable.
		e.repeatCount++

		if e.repeatCount >= rleThreshold {
			// we've seen this a couple of times, we're certainly going to write
			// an rle-run, so just keep on counting repeats for now.
			return nil
		}
	} else {
		// This is a new value, check if it signals the end of an rle-run.
		if e.repeatCount >= rleThreshold {
			// it does! write an rle-run
			if err := e.writeRLERun(); err != nil {
				return err
			}
		}

		e.repeatCount = 1   // This is a new value so we've only seen it once.
		e.previousValue = v // // start tracking this value for repeats.
	}

	// We have not seen enough repeats to justify an rle-run yet,
	// so buffer this value in case we decide to write a bit-packed-run.
	e.valuesBuffer[e.valuesBufferPos] = v
	e.valuesBufferPos++

	if e.valuesBufferPos == rleThreshold {
		// we've encountered less than rleThreshold repeated values, so either start
		// a new bit-packed-run or append to the current bit-packed-run.
		if err := e.writeOrAppendBitPackedRun(); err != nil {
			return err
		}
	}

	return nil
}

func (e *HybridEncoder) Append(data []int32) error {
	for i := range data {
		if err := e.AppendSingle(data[i]); err != nil {
			return err
		}
	}

	return nil
}

func (e *HybridEncoder) Write(writer io.Writer) error {
	if e.repeatCount >= rleThreshold {
		if err := e.writeRLERun(); err != nil {
			return err
		}
	} else if e.valuesBufferPos > 0 {
		for i := e.valuesBufferPos; i < rleBufSize; i++ {
			e.valuesBuffer[i] = 0
		}

		if err := e.writeOrAppendBitPackedRun(); err != nil {
			return err
		}
		e.endPreviousBitPackedRun()
	} else {
		e.endPreviousBitPackedRun()
	}

	return writeFull(writer, e.encodingBuffer.Bytes())
}

func (e *HybridEncoder) writeOrAppendBitPackedRun() error {
	if e.bitPackedGroupCount >= rleMaxBitPackedCount {
		// we've packed as many values as we can for this run, end it and start a new one.
		e.endPreviousBitPackedRun()
	}

	e.bitPackingRun = true

	e.packBuffer = append(e.packBuffer, e.packValues(e.valuesBuffer)...)
	e.valuesBufferPos = 0

	// clear the repeat count, as some repeated values
	// may have just been bit packed into this run.
	e.repeatCount = 0

	e.bitPackedGroupCount++

	return nil
}

func (e *HybridEncoder) endPreviousBitPackedRun() {
	if !e.bitPackingRun {
		return
	}

	// create bit-packed-header, which needs to fit in 1 byte.
	bitPackHeader := (e.bitPackedGroupCount << 1) | 1
	buf := make([]byte, 4)
	l := binary.PutUvarint(buf, uint64(bitPackHeader))

	e.encodingBuffer.Write(buf[:l])
	e.encodingBuffer.Write(e.packBuffer)

	// mark that this run is over and reset the number of groups
	//e.bitPackedRunHeaderPointer = nil
	e.bitPackingRun = false
	e.bitPackedGroupCount = 0
}

func (e *HybridEncoder) writeRLERun() (err error) {
	// we may have been working on a bit-packed-run so close that run if
	// it exists before writing this rle-run.
	e.endPreviousBitPackedRun()

	// write the rle-header (lsb of 0 signifies a rle run)
	if writeErr := writeUVarInt64(&e.encodingBuffer, uint64(e.repeatCount<<1)); writeErr != nil {
		return writeErr
	}

	// write the repeated-value
	switch (e.bitWidth + 7) / 8 {
	case 0:
		return nil
	case 1:
		err = writeIntLittleEndianOnOneByte(&e.encodingBuffer, int64(e.previousValue))
	case 2:
		err = writeIntLittleEndianOnTwoByte(&e.encodingBuffer, int64(e.previousValue))
	case 3:
		err = writeIntLittleEndianOnThreeByte(&e.encodingBuffer, int64(e.previousValue))
	case 4:
		err = writeIntLittleEndianOnFourByte(&e.encodingBuffer, int64(e.previousValue))
	default:
		return errors.WithFields(
			errors.WithStack(errInvalidBitWidth),
			errors.Fields{
				"bit-width": e.bitWidth,
			})
	}

	if err != nil {
		return err
	}

	// reset the repeat count
	e.repeatCount = 0

	// throw away all the buffered values, they were just repeats and they've been written
	e.valuesBufferPos = 0

	return nil
}
