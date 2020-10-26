package encoding

import (
	"testing"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/source/memory"
	"github.com/hexbee-net/parquet/tests/fakes"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

// Int32 ///////////////////////////////////////////////////////////////////////

func TestDeltaBinaryPackEncoder32(t *testing.T) {
	t.Run("NewDeltaBinaryPackEncoder32", TestNewDeltaBinaryPackEncoder32)
	t.Run("Init", TestDeltaBinaryPackEncoder32_Init)
	t.Run("Init - NilWriter", TestDeltaBinaryPackEncoder32_Init_NilWriter)
	t.Run("Init - InvalidBlockSize", TestDeltaBinaryPackEncoder32_Init_InvalidBlockSize)
	t.Run("Init - InvalidBlockCount", TestDeltaBinaryPackEncoder32_Init_InvalidBlockCount)
	t.Run("AddInt32", TestDeltaBinaryPackEncoder32_AddInt32)
	t.Run("AddInt32 - MultiBlock", TestDeltaBinaryPackEncoder32_AddInt32_MultiBlock)
	t.Run("Close", TestDeltaBinaryPackEncoder32_Close)
	t.Run("Close - WriteFail", TestDeltaBinaryPackEncoder32_Close_WriteFail)
	t.Run("flush32", TestDeltaBinaryPackEncoder32_flush32)
}

func TestNewDeltaBinaryPackEncoder32(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	assert.NotNil(t, encoder)
}

func TestDeltaBinaryPackEncoder32_Init(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	err := encoder.Init(memory.NewWriter(nil))

	assert.NoError(t, err)
}

func TestDeltaBinaryPackEncoder32_Init_NilWriter(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder32(128, 4)

	err := encoder.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilWriter.Error())
}

func TestDeltaBinaryPackEncoder32_Init_InvalidBlockSize(t *testing.T) {
	t.Parallel()

	sizes := []int{
		-1,
		129,
	}

	for _, size := range sizes {
		encoder := NewDeltaBinaryPackEncoder32(size, 4)

		err := encoder.Init(memory.NewWriter(nil))

		assert.EqualError(t, errors.Cause(err), errInvalidBlockSize.Error())
	}
}

func TestDeltaBinaryPackEncoder32_Init_InvalidBlockCount(t *testing.T) {
	t.Parallel()

	counts := []int{
		-1,
		3,
		8,
	}

	for _, count := range counts {
		encoder := NewDeltaBinaryPackEncoder32(128, count)

		err := encoder.Init(memory.NewWriter(nil))

		assert.EqualError(t, errors.Cause(err), errInvalidMiniblockCount.Error())
	}
}

func TestDeltaBinaryPackEncoder32_AddInt32(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	require.NoError(t, encoder.Init(writer))

	require.NoError(t, encoder.AddInt32(7))
	require.NoError(t, encoder.AddInt32(5))
	require.NoError(t, encoder.AddInt32(3))
	require.NoError(t, encoder.AddInt32(1))
	require.NoError(t, encoder.AddInt32(2))
	require.NoError(t, encoder.AddInt32(3))
	require.NoError(t, encoder.AddInt32(4))
	require.NoError(t, encoder.AddInt32(5))
}

func TestDeltaBinaryPackEncoder32_AddInt32_MultiBlock(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	require.NoError(t, encoder.Init(writer))

	for i := 0; i < 140; i++ {
		require.NoError(t, encoder.AddInt32(int32(i)))
	}
}

func TestDeltaBinaryPackEncoder32_Close(t *testing.T) {
	t.Parallel()

	values := []int32{7, 5, 3, 1, 2, 3, 4, 5}

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	require.NoError(t, encoder.Init(writer))

	for _, v := range values {
		require.NoError(t, encoder.AddInt32(v))
	}

	require.NoError(t, encoder.Close())

	assert.ElementsMatch(t, []byte{
		128, 1, 4, 8, 14,
		3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0,
	}, writer.Bytes())
}

func TestDeltaBinaryPackEncoder32_Close_WriteFail(t *testing.T) {
	t.Parallel()

	writer := fakes.NewWriterMock(t)
	writer.WriteMock.Return(0, errors.New("write failed"))

	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	err := encoder.Init(writer)
	require.NoError(t, err)

	err = encoder.Close()
	assert.EqualError(t, errors.Cause(err), "write failed")
}

func TestDeltaBinaryPackEncoder32_flush32(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder32(128, 4)
	require.NoError(t, encoder.Init(writer))

	encoder.deltas = []int32{-2, -2, -2, 1, 1, 1, 1}
	encoder.minDelta = -2

	err := encoder.flush32()
	require.NoError(t, err)

	assert.ElementsMatch(t, []byte{3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0}, encoder.buffer.Bytes())
}

// Int64 ///////////////////////////////////////////////////////////////////////

func TestDeltaBinaryPackEncoder64(t *testing.T) {
	t.Run("NewDeltaBinaryPackEncoder64", TestNewDeltaBinaryPackEncoder64)
	t.Run("Init", TestDeltaBinaryPackEncoder64_Init)
	t.Run("Init - NilWriter", TestDeltaBinaryPackEncoder64_Init_NilWriter)
	t.Run("Init - InvalidBlockSize", TestDeltaBinaryPackEncoder64_Init_InvalidBlockSize)
	t.Run("Init - InvalidBlockCount", TestDeltaBinaryPackEncoder64_Init_InvalidBlockCount)
	t.Run("AddInt64", TestDeltaBinaryPackEncoder64_AddInt64)
	t.Run("AddInt64 - MultiBlock", TestDeltaBinaryPackEncoder64_AddInt64_MultiBlock)
	t.Run("Close", TestDeltaBinaryPackEncoder64_Close)
	t.Run("Close - WriteFail", TestDeltaBinaryPackEncoder64_Close_WriteFail)
	t.Run("flush64", TestDeltaBinaryPackEncoder64_flush64)
}

func TestNewDeltaBinaryPackEncoder64(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	assert.NotNil(t, encoder)
}

func TestDeltaBinaryPackEncoder64_Init(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	err := encoder.Init(memory.NewWriter(nil))

	assert.NoError(t, err)
}

func TestDeltaBinaryPackEncoder64_Init_NilWriter(t *testing.T) {
	t.Parallel()

	encoder := NewDeltaBinaryPackEncoder64(128, 4)

	err := encoder.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilWriter.Error())
}

func TestDeltaBinaryPackEncoder64_Init_InvalidBlockSize(t *testing.T) {
	t.Parallel()

	sizes := []int{
		-1,
		129,
	}

	for _, size := range sizes {
		encoder := NewDeltaBinaryPackEncoder64(size, 4)

		err := encoder.Init(memory.NewWriter(nil))

		assert.EqualError(t, errors.Cause(err), errInvalidBlockSize.Error())
	}
}

func TestDeltaBinaryPackEncoder64_Init_InvalidBlockCount(t *testing.T) {
	t.Parallel()

	counts := []int{
		-1,
		3,
		8,
	}

	for _, count := range counts {
		encoder := NewDeltaBinaryPackEncoder64(128, count)

		err := encoder.Init(memory.NewWriter(nil))

		assert.EqualError(t, errors.Cause(err), errInvalidMiniblockCount.Error())
	}
}

func TestDeltaBinaryPackEncoder64_AddInt64(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	require.NoError(t, encoder.Init(writer))

	require.NoError(t, encoder.AddInt64(7))
	require.NoError(t, encoder.AddInt64(5))
	require.NoError(t, encoder.AddInt64(3))
	require.NoError(t, encoder.AddInt64(1))
	require.NoError(t, encoder.AddInt64(2))
	require.NoError(t, encoder.AddInt64(3))
	require.NoError(t, encoder.AddInt64(4))
	require.NoError(t, encoder.AddInt64(5))
}

func TestDeltaBinaryPackEncoder64_AddInt64_MultiBlock(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	require.NoError(t, encoder.Init(writer))

	for i := 0; i < 140; i++ {
		require.NoError(t, encoder.AddInt64(int64(i)))
	}
}

func TestDeltaBinaryPackEncoder64_Close(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	require.NoError(t, encoder.Init(writer))

	require.NoError(t, encoder.AddInt64(7))
	require.NoError(t, encoder.AddInt64(5))
	require.NoError(t, encoder.AddInt64(3))
	require.NoError(t, encoder.AddInt64(1))
	require.NoError(t, encoder.AddInt64(2))
	require.NoError(t, encoder.AddInt64(3))
	require.NoError(t, encoder.AddInt64(4))
	require.NoError(t, encoder.AddInt64(5))

	require.NoError(t, encoder.Close())

	assert.ElementsMatch(t, []byte{
		128, 1, 4, 8, 14,
		3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0,
	}, writer.Bytes())
}

func TestDeltaBinaryPackEncoder64_Close_WriteFail(t *testing.T) {
	t.Parallel()

	writer := fakes.NewWriterMock(t)
	writer.WriteMock.Return(0, errors.New("write failed"))

	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	err := encoder.Init(writer)
	require.NoError(t, err)

	err = encoder.Close()
	assert.EqualError(t, errors.Cause(err), "write failed")
}

func TestDeltaBinaryPackEncoder64_flush64(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)
	encoder := NewDeltaBinaryPackEncoder64(128, 4)
	require.NoError(t, encoder.Init(writer))

	encoder.deltas = []int64{-2, -2, -2, 1, 1, 1, 1}
	encoder.minDelta = -2

	err := encoder.flush64()
	require.NoError(t, err)

	assert.ElementsMatch(t, []byte{3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0}, encoder.buffer.Bytes())
}
