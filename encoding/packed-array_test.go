package encoding

import (
	"testing"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/source/memory"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

func TestPackedArray(t *testing.T) {
	t.Run("Reset", TestPackedArray_Reset)
	t.Run("Reset_InvalidBitWidth", TestPackedArray_Reset_InvalidBitWidth)
	t.Run("AppendSingle_Once", TestPackedArray_AppendSingle_Once)
	t.Run("AppendSingle_Multiple", TestPackedArray_AppendSingle_Multiple)
	t.Run("AppendArray", TestPackedArray_AppendArray)
	t.Run("AppendArray_DifferentBitWidths", TestPackedArray_AppendArray_DifferentBitWidths)
	t.Run("AppendArray_NilSource", TestPackedArray_AppendArray_NilSource)
	t.Run("Count", TestPackedArray_Count)
	t.Run("At", TestPackedArray_At)
	t.Run("At_ZeroBitWidth", TestPackedArray_At_ZeroBitWidth)
	t.Run("At_OutOfRange", TestPackedArray_At_OutOfRange)
	t.Run("Flush", TestPackedArray_Flush)
	t.Run("Write", TestPackedArray_Write)
}

func TestPackedArray_Reset(t *testing.T) {
	t.Parallel()

	a := PackedArray{}

	err := a.Reset(32)

	assert.NoError(t, err)
}

func TestPackedArray_Reset_InvalidBitWidth(t *testing.T) {
	t.Parallel()

	a := PackedArray{}

	err := a.Reset(48)

	assert.EqualError(t, errors.Cause(err), errInvalidBitWidth.Error())
}

func TestPackedArray_AppendSingle_Once(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	a.AppendSingle(1)
	assert.Equal(t, 1, a.count)
}

func TestPackedArray_AppendSingle_Multiple(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	for i := 1; i <= packedArrayBufSize*2; i++ {
		a.AppendSingle(int32(i))
		assert.Equal(t, i, a.count)
	}
}

func TestPackedArray_AppendArray(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	b := PackedArray{}
	require.NoError(t, b.Reset(32))
	b.AppendSingle(1)

	err := a.AppendArray(&b)

	require.NoError(t, err)
	assert.Equal(t, 1, a.count)
	assert.ElementsMatch(t, b.buf, a.buf)
}

func TestPackedArray_AppendArray_DifferentBitWidths(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	b := PackedArray{}
	require.NoError(t, b.Reset(16))
	b.AppendSingle(1)

	err := a.AppendArray(&b)

	assert.EqualError(t, errors.Cause(err), "cannot append array with different bit-width")
	assert.Equal(t, a.count, 0)
}

func TestPackedArray_AppendArray_NilSource(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	err := a.AppendArray(nil)

	assert.EqualError(t, errors.Cause(err), "source array is nil")
}

func TestPackedArray_Count(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	assert.Equal(t, 0, a.Count())
	a.AppendSingle(1)
	assert.Equal(t, 1, a.Count())
}

func TestPackedArray_At(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	a.AppendSingle(1)
	require.Equal(t, 1, a.count)

	v, err := a.At(0)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), v)
}

func TestPackedArray_At_ZeroBitWidth(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(0))

	a.AppendSingle(1)
	require.Equal(t, 1, a.count)

	v, err := a.At(0)

	assert.NoError(t, err)
	assert.Zero(t, v)
}

func TestPackedArray_At_OutOfRange(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	a.AppendSingle(1)
	require.Equal(t, 1, a.count)

	v, err := a.At(1)

	assert.EqualError(t, errors.Cause(err), errOutOfRange.Error())
	assert.Zero(t, v)
}

func TestPackedArray_Flush(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	res := make([]int32, 0, packedArrayBufSize)
	for i := 1; i <= packedArrayBufSize; i++ {
		a.AppendSingle(int32(i))
		res = append(res, int32(i))
	}

	assert.ElementsMatch(t, res, a.buf)
	a.Flush()

	assert.Equal(t, 0, a.bufPos)
}

func TestPackedArray_Flush_Multiple(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(32))

	res := make([]int32, 0, packedArrayBufSize)
	for i := 1; i <= packedArrayBufSize; i++ {
		a.AppendSingle(int32(i))
		res = append(res, int32(i))
	}

	assert.ElementsMatch(t, res, a.buf)
	a.Flush()

	for i := 1; i <= packedArrayBufSize/2; i++ {
		a.AppendSingle(int32(i))
	}
	a.Flush()

	assert.Equal(t, 0, a.bufPos)
}

func TestPackedArray_Write(t *testing.T) {
	t.Parallel()

	a := PackedArray{}
	require.NoError(t, a.Reset(3))

	input := []int32{0, 1, 2, 3, 4, 5, 6, 7}

	for _, i := range input {
		a.AppendSingle(i)
	}

	writer := memory.NewWriter(nil)

	a.Flush()
	err := a.Write(writer)

	require.NoError(t, err)
	require.ElementsMatch(t, []byte{0b10001000, 0b11000110, 0b11111010}, writer.Bytes())
}
