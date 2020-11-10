package encoding

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/source/memory"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

func TestHybridEncoder_RLEOnly(t *testing.T) {
	e, err := NewHybridEncoder(3)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		require.NoError(t, e.AppendSingle(4))
	}
	for i := 0; i < 100; i++ {
		require.NoError(t, e.AppendSingle(5))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = 100 << 1 = 200
	v, err := readVarUInt64(reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), v)

	// payload = 4
	r, err := readIntLittleEndianOnOneByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(4), r)

	// header = 100 << 1 = 200
	v, err = readVarUInt64(reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), v)

	// payload = 5
	r, err = readIntLittleEndianOnOneByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(5), r)

	// EOF
	_, err = readIntLittleEndianOnOneByte(reader)
	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestHybridEncoder_RepeatedZeros(t *testing.T) {
	e, err := NewHybridEncoder(3)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, e.AppendSingle(0))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = 10 << 1 = 20
	v, err := readVarUInt64(reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), v)

	// payload = 0
	r, err := readIntLittleEndianOnOneByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(0), r)
}

func TestHybridEncoder_BitWidthZeros(t *testing.T) {
	e, err := NewHybridEncoder(0)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, e.AppendSingle(0))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = 10 << 1 = 20
	v, err := readVarUInt64(reader)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), v)

	// EOF
	_, err = readIntLittleEndianOnOneByte(reader)
	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestHybridEncoder_BitPackingOnly(t *testing.T) {
	const bitWidth = 3

	e, err := NewHybridEncoder(bitWidth)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		require.NoError(t, e.AppendSingle(int32(i%3)))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = ((104/8) << 1) | 1 = 27
	v, err := readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(27), v)

	all, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	expected := []byte{136, 16, 33, 66, 132, 8, 17, 34, 68, 136, 16, 33, 66, 132, 8, 17, 34, 68, 136, 16, 33, 66, 132, 8, 17, 34, 68, 136, 16, 33, 66, 132, 8, 17, 34, 68, 136, 0, 0}
	assert.Equal(t, len(expected), len(all), "check result length")
	assert.ElementsMatch(t, expected, all, "check result content")

	for i := 0; i < 100; i++ {
		assert.Equal(t, int32(i%3), readAt(t, all, bitWidth, i), "check value at index %d", i)
	}
}

func TestHybridEncoder_BitPackingOverflow(t *testing.T) {
	const (
		bitWidth  = 3
		maxValues = (127 >> 1) * 8 // 504 is the max number of values in a bit packed run that still has a header of 1 byte
	)

	e, err := NewHybridEncoder(bitWidth)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		require.NoError(t, e.AppendSingle(int32(i%3)))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	v, err := readUVarInt32(reader)
	require.NoError(t, err)

	assert.Equal(t, int32(((maxValues/8)<<1)|1), v)

	all, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	for i := 0; i < maxValues; i++ {
		assert.Equal(t, int32(i%3), readAt(t, all, bitWidth, i))
	}
}

func TestHybridEncoder_TransitionFromBitPackingToRLE(t *testing.T) {
	const bitWidth = 3

	e, err := NewHybridEncoder(bitWidth)
	require.NoError(t, err)

	// 5 obviously bit-packed values
	require.NoError(t, e.AppendSingle(0))
	require.NoError(t, e.AppendSingle(1))
	require.NoError(t, e.AppendSingle(0))
	require.NoError(t, e.AppendSingle(1))
	require.NoError(t, e.AppendSingle(0))

	// three repeated values, that ought to be bit-packed as well
	require.NoError(t, e.AppendSingle(2))
	require.NoError(t, e.AppendSingle(2))
	require.NoError(t, e.AppendSingle(2))

	// lots more repeated values, that should be rle-encoded
	for i := 0; i < 100; i++ {
		require.NoError(t, e.AppendSingle(2))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	v, err := readUVarInt32(reader)
	require.NoError(t, err)

	// header = ((8/8) << 1) | 1 = 3
	assert.Equal(t, int32(3), v)

	int32s, err := unpack(t, bitWidth, 8, reader)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int32{0, 1, 0, 1, 0, 2, 2, 2}, int32s)

	v, err = readUVarInt32(reader)
	require.NoError(t, err)

	// header = 100 << 1 = 200
	assert.Equal(t, int32(200), v)

	// payload = 2
	r, err := readIntLittleEndianOnOneByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(2), r)

	// EOF
	_, err = readIntLittleEndianOnOneByte(reader)
	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestHybridEncoder_PaddingZerosOnUnfinishedBitPackedRuns(t *testing.T) {
	e, err := NewHybridEncoder(5)
	require.NoError(t, err)

	for i := 0; i < 9; i++ {
		require.NoError(t, e.AppendSingle(int32(i+1)))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = ((16/8) << 1) | 1 = 5
	v, err := readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(5), v)

	int32s, err := unpack(t, 5, 16, reader)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0}, int32s)

	// EOF
	_, err = readIntLittleEndianOnOneByte(reader)
	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestHybridEncoder_SwitchingModes(t *testing.T) {
	e, err := NewHybridEncoder(9)
	require.NoError(t, err)

	// rle first
	for i := 0; i < 25; i++ {
		require.NoError(t, e.AppendSingle(17))
	}

	// bit-packing
	for i := 0; i < 7; i++ {
		require.NoError(t, e.AppendSingle(7))
	}

	require.NoError(t, e.AppendSingle(8))
	require.NoError(t, e.AppendSingle(9))
	require.NoError(t, e.AppendSingle(10))

	// bit-packing followed by rle
	for i := 0; i < 25; i++ {
		require.NoError(t, e.AppendSingle(6))
	}

	// followed by a different rle
	for i := 0; i < 8; i++ {
		require.NoError(t, e.AppendSingle(5))
	}

	writer := memory.NewWriter(nil)
	require.NoError(t, e.Write(writer))

	reader := bytes.NewReader(writer.Bytes())

	// header = 25 << 1 = 50
	v, err := readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(50), v)

	// payload = 17, stored in 2 bytes
	r, err := readIntLittleEndianOnTwoByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(17), r)

	// header = ((16/8) << 1) | 1 = 5
	v, err = readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(5), v)

	int32s, err := unpack(t, 9, 16, reader)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int32{7, 7, 7, 7, 7, 7, 7, 8, 9, 10, 6, 6, 6, 6, 6, 6}, int32s)

	// header = 19 << 1 = 38
	v, err = readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(38), v)

	// payload = 6, stored in 2 bytes
	r, err = readIntLittleEndianOnTwoByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(6), r)

	// header = 8 << 1  = 16
	v, err = readUVarInt32(reader)
	require.NoError(t, err)
	assert.Equal(t, int32(16), v)

	// payload = 5, stored in 2 bytes
	r, err = readIntLittleEndianOnTwoByte(reader)
	require.NoError(t, err)
	assert.Equal(t, int64(5), r)

	// EOF
	_, err = readIntLittleEndianOnOneByte(reader)
	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func readAt(t *testing.T, b []byte, bitWidth, pos int) int32 {
	t.Helper()

	if bitWidth == 0 {
		return 0
	}

	reader := unpack8Int32FuncByWidth[bitWidth]

	block := (pos / rleBufSize) * bitWidth
	idx := pos % rleBufSize

	buf := reader(b[block : block+bitWidth])

	return buf[idx]
}

func unpack(t *testing.T, bitWidth, numValues int, r io.Reader) ([]int32, error) {
	t.Helper()

	if bitWidth == 0 {
		return []int32{}, nil
	}

	unpacker := unpack8Int32FuncByWidth[bitWidth]

	res := make([]int32, 0)
	numGroups := numValues / 8

	for i := 0; i < numGroups; i++ {
		packedBuf := make([]byte, bitWidth)
		read, err := r.Read(packedBuf)

		if err != nil {
			return nil, err
		}

		if read != bitWidth {
			return nil, io.EOF
		}

		unpackedBuf := unpacker(packedBuf)
		res = append(res, unpackedBuf[:]...)
	}

	return res, nil
}
