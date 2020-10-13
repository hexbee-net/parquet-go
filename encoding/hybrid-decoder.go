package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math/bits"

	"github.com/hexbee-net/errors"
)

type HybridDecoder struct {
	r io.Reader

	bitWidth     int
	unpackerFn   unpack8int32Func
	rleValueSize int

	bpRun [8]int32

	rleCount uint32
	rleValue int32

	bpCount  uint32
	bpRunPos uint8

	buffered bool
}

func NewHybridDecoder(bitWidth int, buffered bool) *HybridDecoder {
	return &HybridDecoder{
		bitWidth:   bitWidth,
		unpackerFn: unpack8Int32FuncByWidth[bitWidth],

		rleValueSize: (bitWidth + 7) / 8,

		buffered: buffered,
	}
}

func (d *HybridDecoder) Init(reader io.Reader) error {
	if d.buffered {
		buf, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}

		d.r = bytes.NewReader(buf)
	} else {
		d.r = reader
	}

	return nil
}

func (d *HybridDecoder) InitSize(reader io.Reader) error {
	if d.bitWidth == 0 {
		return nil
	}

	var size uint32
	if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
		return err
	}

	return d.Init(io.LimitReader(reader, int64(size)))
}

func (d *HybridDecoder) Next() (int32, error) {
	var next int32

	// when the bit width is zero, it means we can only have infinite zero.
	if d.bitWidth == 0 {
		return 0, nil
	}

	if d.r == nil {
		return 0, errors.New("reader is not initialized")
	}

	if d.rleCount == 0 && d.bpCount == 0 && d.bpRunPos == 0 {
		if err := d.readRunHeader(); err != nil {
			return 0, err
		}
	}

	switch {
	case d.rleCount > 0:
		next = d.rleValue
		d.rleCount--

	case d.bpCount > 0 || d.bpRunPos > 0:
		if d.bpRunPos == 0 {
			if err := d.readBitPackedRun(); err != nil {
				return 0, err
			}
			d.bpCount--
		}

		next = d.bpRun[d.bpRunPos]
		d.bpRunPos = (d.bpRunPos + 1) % 8

	default:
		return 0, io.EOF
	}

	return next, nil
}

func (d *HybridDecoder) readRunHeader() error {
	h, err := readUVariant32(d.r)
	if err != nil {
		// this error could be EOF which is ok by this implementation the only issue is the binary.ReadUVariant can not
		// return UnexpectedEOF is there is some bit read from the stream with no luck, it always return EOF
		return err
	}

	// The lower bit indicate if this is bitpack or rle
	if h&1 == 1 {
		d.bpCount = uint32(h >> 1)
		if d.bpCount == 0 {
			return errors.New("rle: empty bit-packed run")
		}

		d.bpRunPos = 0
	} else {
		d.rleCount = uint32(h >> 1)
		if d.rleCount == 0 {
			return errors.New("rle: empty RLE run")
		}
		return d.readRLERunValue()
	}

	return nil
}

func (d *HybridDecoder) readBitPackedRun() error {
	data := make([]byte, d.bitWidth)

	_, err := d.r.Read(data)
	if err != nil {
		return err
	}

	d.bpRun = d.unpackerFn(data)

	return nil
}

func (d *HybridDecoder) readRLERunValue() error {
	v := make([]byte, d.rleValueSize)

	n, err := d.r.Read(v)
	if err != nil {
		return err
	}

	if n != d.rleValueSize {
		return io.ErrUnexpectedEOF
	}

	d.rleValue = decodeRLEValue(v)

	if bits.LeadingZeros32(uint32(d.rleValue)) < 32-d.bitWidth {
		return errors.New("rle: RLE run value is too large")
	}

	return nil
}

func decodeRLEValue(value []byte) int32 {
	switch len(value) {
	case 0: //nolint:gomnd // the switch is on the size of the input
		return 0
	case 1: //nolint:gomnd // the switch is on the size of the input
		return int32(value[0])
	case 2: //nolint:gomnd // the switch is on the size of the input
		return int32(value[0]) + int32(value[1])<<8
	case 3: //nolint:gomnd // the switch is on the size of the input
		return int32(value[0]) + int32(value[1])<<8 + int32(value[2])<<16
	case 4: //nolint:gomnd // the switch is on the size of the input
		return int32(value[0]) + int32(value[1])<<8 + int32(value[2])<<16 + int32(value[3])<<24
	default:
		panic("invalid argument")
	}
}
