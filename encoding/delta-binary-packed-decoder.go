package encoding

import (
	"io"

	"github.com/hexbee-net/errors"
)

// Generic decoder /////////////////////////////////////////////////////////////

type deltaBinaryPackDecoder struct {
	r io.Reader

	blockSize                int32
	miniblockCount           int32
	ValuesCount              int32
	miniBlockValueCount      int32
	miniBlockBitWidth        []uint8
	currentMiniBlock         int32
	currentMiniBlockBitWidth uint8
	miniBlockPosition        int32 // position inside the current mini block
	position                 int32 // position in the value. since delta may have padding we need to track this

	unpackMiniBlock  func(buf []byte)
	setPreviousValue func() (err error)
	readMinDelta     func() (err error)
}

func (d *deltaBinaryPackDecoder) readBlockHeader() (err error) {
	if d.blockSize, err = readUVarInt32(d.r); err != nil {
		return errors.Wrap(err, "failed to read block size")
	}

	if d.blockSize <= 0 || d.blockSize%128 != 0 {
		return errors.WithFields(
			errors.WithStack(errInvalidBlockSize),
			errors.Fields{
				"block-size": d.blockSize,
			})
	}

	if d.miniblockCount, err = readUVarInt32(d.r); err != nil {
		return errors.Wrap(err, "failed to read number of mini blocks")
	}

	if d.miniblockCount <= 0 || d.blockSize%d.miniblockCount != 0 {
		return errors.WithFields(
			errors.WithStack(errInvalidMiniblockCount),
			errors.Fields{
				"miniblock-count": d.miniblockCount,
			})
	}

	d.miniBlockValueCount = d.blockSize / d.miniblockCount

	if d.ValuesCount, err = readUVarInt32(d.r); err != nil {
		return errors.Wrapf(err, "failed to read total value count")
	}

	if d.ValuesCount == 0 {
		return nil
	}

	return d.setPreviousValue()
}

func (d *deltaBinaryPackDecoder) readMiniBlockHeader() error {
	if err := d.readMinDelta(); err != nil {
		return err
	}

	// the mini block bit-width is always there, even if the value is zero
	d.miniBlockBitWidth = make([]uint8, d.miniblockCount)
	if _, err := io.ReadFull(d.r, d.miniBlockBitWidth); err != nil {
		return errors.Wrap(err, "not enough data to read all miniblock bit widths")
	}

	for i := range d.miniBlockBitWidth {
		const maxMiniblockBitWidth = 32
		if d.miniBlockBitWidth[i] > maxMiniblockBitWidth {
			return errors.WithFields(
				errors.New("invalid miniblock bit-width"),
				errors.Fields{
					"miniblock-index": i,
					"bit-width":       d.miniBlockBitWidth[i],
				})
		}
	}

	// start from the first min block in a big block
	d.currentMiniBlock = 0

	return nil
}

func (d *deltaBinaryPackDecoder) next() (err error) {
	if d.position >= d.ValuesCount {
		// No value left in the buffer
		return io.EOF
	}

	// need new byte?
	if d.position%8 == 0 {
		if err := d.advanceBlock(); err != nil {
			return err
		}

		// read next 8 values
		bw := int32(d.currentMiniBlockBitWidth)

		buf := make([]byte, bw)
		if _, err := io.ReadFull(d.r, buf); err != nil {
			return err
		}

		d.unpackMiniBlock(buf)
		d.miniBlockPosition += bw

		// there is padding here, read them all from the reader, first deal with the remaining of the current block,
		// then the next blocks. if the blocks bit width is zero then simply ignore them, but the docs said reader
		// should accept any arbitrary bit width here.
		if err := d.readPadding(bw); err != nil {
			return err
		}
	}

	return nil
}

func (d *deltaBinaryPackDecoder) advanceBlock() error {
	// do we need to advance a mini block?
	if d.position%d.miniBlockValueCount == 0 {
		// do we need to advance a big block?
		if d.currentMiniBlock >= d.miniblockCount {
			if err := d.readMiniBlockHeader(); err != nil {
				return err
			}
		}

		d.currentMiniBlockBitWidth = d.miniBlockBitWidth[d.currentMiniBlock]

		d.miniBlockPosition = 0
		d.currentMiniBlock++
	}

	return nil
}

func (d *deltaBinaryPackDecoder) readPadding(w int32) error {
	if d.position+8 >= d.ValuesCount {
		//  current block
		l := (d.miniBlockValueCount/8)*w - d.miniBlockPosition
		if l < 0 {
			return errors.New("invalid stream")
		}

		remaining := make([]byte, l)
		_, _ = io.ReadFull(d.r, remaining)

		for i := d.currentMiniBlock; i < d.miniblockCount; i++ {
			bw := int32(d.miniBlockBitWidth[d.currentMiniBlock])
			if bw != 0 {
				remaining := make([]byte, (d.miniBlockValueCount/8)*bw)
				_, _ = io.ReadFull(d.r, remaining)
			}
		}
	}

	return nil
}

// Int32 ///////////////////////////////////////////////////////////////////////

type DeltaBinaryPackDecoder32 struct {
	deltaBinaryPackDecoder

	previousValue int32
	minDelta      int32

	miniBlockInt32 [8]int32
}

func (d *DeltaBinaryPackDecoder32) Init(reader io.Reader) error {
	if reader == nil {
		return errors.WithStack(errNilReader)
	}

	d.r = reader

	d.unpackMiniBlock = d.unpackMiniBlock32
	d.setPreviousValue = d.setPreviousValue32
	d.readMinDelta = d.readMinDelta32

	if err := d.readBlockHeader(); err != nil {
		return err
	}

	if d.ValuesCount == 0 {
		return nil
	}

	if err := d.readMiniBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *DeltaBinaryPackDecoder32) InitSize(reader io.Reader) error {
	return d.Init(reader)
}

func (d *DeltaBinaryPackDecoder32) Next() (int32, error) {
	if err := d.deltaBinaryPackDecoder.next(); err != nil {
		return 0, err
	}

	// value is the previous value + delta stored in the reader and the min delta for the block, also we always read one
	// value ahead
	ret := d.previousValue
	d.previousValue += d.miniBlockInt32[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}

func (d *DeltaBinaryPackDecoder32) unpackMiniBlock32(buf []byte) {
	unpack := unpack8Int32FuncByWidth[int(d.currentMiniBlockBitWidth)]
	d.miniBlockInt32 = unpack(buf)
}

func (d *DeltaBinaryPackDecoder32) setPreviousValue32() (err error) {
	if d.previousValue, err = readVarInt32(d.r); err != nil {
		return errors.Wrap(err, "failed to read first value")
	}

	return nil
}

func (d *DeltaBinaryPackDecoder32) readMinDelta32() (err error) {
	if d.minDelta, err = readVarInt32(d.r); err != nil {
		return errors.Wrap(err, "failed to read min delta")
	}

	return nil
}

// Int64 ///////////////////////////////////////////////////////////////////////

type DeltaBinaryPackDecoder64 struct {
	deltaBinaryPackDecoder

	previousValue int64
	minDelta      int64

	miniBlockInt64 [8]int64
}

func (d *DeltaBinaryPackDecoder64) Init(reader io.Reader) error {
	if reader == nil {
		return errors.WithStack(errNilReader)
	}

	d.r = reader

	d.unpackMiniBlock = d.unpackMiniBlock64
	d.setPreviousValue = d.setPreviousValue64
	d.readMinDelta = d.readMinDelta64

	if err := d.readBlockHeader(); err != nil {
		return err
	}

	if d.ValuesCount == 0 {
		return nil
	}

	if err := d.readMiniBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *DeltaBinaryPackDecoder64) InitSize(reader io.Reader) error {
	return d.Init(reader)
}

func (d *DeltaBinaryPackDecoder64) Next() (int64, error) {
	if err := d.deltaBinaryPackDecoder.next(); err != nil {
		return 0, err
	}

	// value is the previous value + delta stored in the reader and the min delta for the block, also we always read one
	// value ahead
	ret := d.previousValue
	d.previousValue += d.miniBlockInt64[d.position%8] + d.minDelta
	d.position++

	return ret, nil
}

func (d *DeltaBinaryPackDecoder64) unpackMiniBlock64(buf []byte) {
	unpack := unpack8Int64FuncByWidth[int(d.currentMiniBlockBitWidth)]
	d.miniBlockInt64 = unpack(buf)
}

func (d *DeltaBinaryPackDecoder64) setPreviousValue64() (err error) {
	if d.previousValue, err = readVarInt64(d.r); err != nil {
		return errors.Wrap(err, "failed to read first value")
	}

	return nil
}

func (d *DeltaBinaryPackDecoder64) readMinDelta64() (err error) {
	if d.minDelta, err = readVarInt64(d.r); err != nil {
		return errors.Wrap(err, "failed to read min delta")
	}

	return nil
}
