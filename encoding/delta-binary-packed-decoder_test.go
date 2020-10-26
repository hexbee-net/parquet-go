package encoding

import (
	"bytes"
	"io"
	"testing"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/source/memory"
	"github.com/hexbee-net/parquet/tests/fakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Int32 ///////////////////////////////////////////////////////////////////////

func TestDeltaBinaryPackDecoder32(t *testing.T) {
	t.Run("Init", TestDeltaBinaryPackDecoder32_Init)
	t.Run("Init_NilReader", TestDeltaBinaryPackDecoder32_Init_NilReader)
	t.Run("Init_EmptyBuffer", TestDeltaBinaryPackDecoder32_Init_EmptyBuffer)
	t.Run("Init_InvalidBlockSize", TestDeltaBinaryPackDecoder32_Init_InvalidBlockSize)
	t.Run("Init_InvalidMiniblockCount", TestDeltaBinaryPackDecoder32_Init_InvalidMiniblockCount)
	t.Run("Init_ReadFail", TestDeltaBinaryPackDecoder32_Init_ReadFail)
	t.Run("InitSize", TestDeltaBinaryPackDecoder32_InitSize)
	t.Run("Next", TestDeltaBinaryPackDecoder32_Next)
	t.Run("Next_NoValues", TestDeltaBinaryPackDecoder32_Next_NoValues)
}

func TestDeltaBinaryPackDecoder32_Init(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder32{}
	err := decoder.Init(reader)

	assert.NoError(t, err)
}

func TestDeltaBinaryPackDecoder32_Init_NilReader(t *testing.T) {
	t.Parallel()

	decoder := DeltaBinaryPackDecoder32{}

	err := decoder.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilReader.Error())
}

func TestDeltaBinaryPackDecoder32_Init_EmptyBuffer(t *testing.T) {
	t.Parallel()

	decoder := DeltaBinaryPackDecoder32{}
	err := decoder.Init(memory.NewWriter(nil))

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestDeltaBinaryPackDecoder32_Init_InvalidBlockSize(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		{0, 1, 4, 0, 0},
		{127, 1, 4, 0, 0},
		{129, 1, 4, 0, 0},
	}

	for _, input := range inputs {
		reader := bytes.NewReader(input)

		decoder := DeltaBinaryPackDecoder32{}
		err := decoder.Init(reader)

		assert.EqualError(t, errors.Cause(err), errInvalidBlockSize.Error())
	}
}

func TestDeltaBinaryPackDecoder32_Init_InvalidMiniblockCount(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		{128, 1, 0, 0, 0},   // 128 / 0 / 0
		{128, 1, 3, 0, 0},   // 128 / 3 / 0
		{128, 1, 128, 2, 0}, // 128 / 256 / 0
	}

	for _, input := range inputs {
		reader := bytes.NewReader(input)

		decoder := DeltaBinaryPackDecoder32{}
		err := decoder.Init(reader)

		assert.EqualError(t, errors.Cause(err), errInvalidMiniblockCount.Error())
	}
}

func TestDeltaBinaryPackDecoder32_Init_ReadFail(t *testing.T) {
	t.Parallel()

	reader := fakes.NewReaderMock(t)
	reader.ReadMock.Return(0, errors.New("read failed"))

	decoder := DeltaBinaryPackDecoder32{}
	err := decoder.Init(reader)

	assert.EqualError(t, errors.Cause(err), "read failed")
}

func TestDeltaBinaryPackDecoder32_InitSize(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder32{}
	err := decoder.InitSize(reader)

	assert.NoError(t, err)
}

func TestDeltaBinaryPackDecoder32_Next(t *testing.T) {
	t.Parallel()

	values := []int32{7, 5, 3, 1, 2, 3, 4, 5}

	reader := bytes.NewReader([]byte{
		128, 1, 4, 8, 14,
		3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0,
	})

	decoder := DeltaBinaryPackDecoder32{}
	require.NoError(t, decoder.Init(reader))

	for _, v := range values {
		r, err := decoder.Next()

		require.NoError(t, err)
		assert.Equal(t, v, r)
	}
}

func TestDeltaBinaryPackDecoder32_Next_NoValues(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder32{}
	require.NoError(t, decoder.Init(reader))

	v, err := decoder.Next()

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
	assert.Equal(t, int32(0), v)
}

// Int64 ///////////////////////////////////////////////////////////////////////

func TestDeltaBinaryPackDecoder64(t *testing.T) {
	t.Run("Init", TestDeltaBinaryPackDecoder64_Init)
	t.Run("Init_NilReader", TestDeltaBinaryPackDecoder64_Init_NilReader)
	t.Run("Init_EmptyBuffer", TestDeltaBinaryPackDecoder64_Init_EmptyBuffer)
	t.Run("Init_InvalidBlockSize", TestDeltaBinaryPackDecoder64_Init_InvalidBlockSize)
	t.Run("Init_InvalidMiniblockCount", TestDeltaBinaryPackDecoder64_Init_InvalidMiniblockCount)
	t.Run("Init_ReadFail", TestDeltaBinaryPackDecoder64_Init_ReadFail)
	t.Run("InitSize", TestDeltaBinaryPackDecoder64_InitSize)
	t.Run("Next", TestDeltaBinaryPackDecoder64_Next)
	t.Run("Next_NoValues", TestDeltaBinaryPackDecoder64_Next_NoValues)
}

func TestDeltaBinaryPackDecoder64_Init(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder64{}
	err := decoder.Init(reader)

	assert.NoError(t, err)
}

func TestDeltaBinaryPackDecoder64_Init_NilReader(t *testing.T) {
	t.Parallel()

	decoder := DeltaBinaryPackDecoder64{}

	err := decoder.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilReader.Error())
}

func TestDeltaBinaryPackDecoder64_Init_EmptyBuffer(t *testing.T) {
	t.Parallel()

	decoder := DeltaBinaryPackDecoder64{}
	err := decoder.Init(memory.NewWriter(nil))

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
}

func TestDeltaBinaryPackDecoder64_Init_InvalidBlockSize(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		{0, 1, 4, 0, 0},
		{127, 1, 4, 0, 0},
		{129, 1, 4, 0, 0},
	}

	for _, input := range inputs {
		reader := bytes.NewReader(input)

		decoder := DeltaBinaryPackDecoder64{}
		err := decoder.Init(reader)

		assert.EqualError(t, errors.Cause(err), errInvalidBlockSize.Error())
	}
}

func TestDeltaBinaryPackDecoder64_Init_InvalidMiniblockCount(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		{128, 1, 0, 0, 0},   // 128 / 0 / 0
		{128, 1, 3, 0, 0},   // 128 / 3 / 0
		{128, 1, 128, 2, 0}, // 128 / 256 / 0
	}

	for _, input := range inputs {
		reader := bytes.NewReader(input)

		decoder := DeltaBinaryPackDecoder64{}
		err := decoder.Init(reader)

		assert.EqualError(t, errors.Cause(err), errInvalidMiniblockCount.Error())
	}
}

func TestDeltaBinaryPackDecoder64_Init_ReadFail(t *testing.T) {
	t.Parallel()

	reader := fakes.NewReaderMock(t)
	reader.ReadMock.Return(0, errors.New("read failed"))

	decoder := DeltaBinaryPackDecoder64{}
	err := decoder.Init(reader)

	assert.EqualError(t, errors.Cause(err), "read failed")
}

func TestDeltaBinaryPackDecoder64_InitSize(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder64{}
	err := decoder.InitSize(reader)

	assert.NoError(t, err)
}

func TestDeltaBinaryPackDecoder64_Next(t *testing.T) {
	t.Parallel()

	values := []int64{7, 5, 3, 1, 2, 3, 4, 5}

	reader := bytes.NewReader([]byte{
		128, 1, 4, 8, 14,
		3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0,
	})

	decoder := DeltaBinaryPackDecoder64{}
	require.NoError(t, decoder.Init(reader))

	for _, v := range values {
		r, err := decoder.Next()

		require.NoError(t, err)
		assert.Equal(t, v, r)
	}
}

func TestDeltaBinaryPackDecoder64_Next_NoValues(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte{128, 1, 4, 0, 0})

	decoder := DeltaBinaryPackDecoder64{}
	require.NoError(t, decoder.Init(reader))

	v, err := decoder.Next()

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
	assert.Equal(t, int64(0), v)
}
