package types

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

// Encoder /////////////////////////////

func TestDoublePlainEncoder(t *testing.T) {
	t.Run("Init", TestDoublePlainEncoder_Init)
	t.Run("Init_NilWriter", TestDoublePlainEncoder_Init_NilWriter)
	t.Run("EncodeValues", TestDoublePlainEncoder_EncodeValues)
	t.Run("EncodeValues_WrongTypes", TestDoublePlainEncoder_EncodeValues_WrongTypes)
	t.Run("Close", TestDoublePlainEncoder_Close)
}

func TestDoublePlainEncoder_Init(t *testing.T) {
	t.Parallel()

	e := DoublePlainEncoder{}

	err := e.Init(memory.NewWriter(nil))

	assert.NoError(t, err)
}

func TestDoublePlainEncoder_Init_NilWriter(t *testing.T) {
	t.Parallel()

	e := DoublePlainEncoder{}

	err := e.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilWriter.Error())
}

func TestDoublePlainEncoder_EncodeValues(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)

	e := DoublePlainEncoder{}
	err := e.Init(writer)
	require.NoError(t, err)

	values := []interface{}{float64(1.), float64(2.), float64(3.)}
	err = e.EncodeValues(values)
	require.NoError(t, err)

	err = e.Close()
	require.NoError(t, err)

	assert.ElementsMatch(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 8, 0x40}, writer.Bytes())
}

func TestDoublePlainEncoder_EncodeValues_WrongTypes(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)

	e := DoublePlainEncoder{}
	err := e.Init(writer)
	require.NoError(t, err)

	values := []interface{}{true, false, true}
	err = e.EncodeValues(values)

	assert.EqualError(t, errors.Cause(err), errInvalidType.Error())
}

func TestDoublePlainEncoder_Close(t *testing.T) {
	t.Parallel()

	e := DoublePlainEncoder{}
	err := e.Init(memory.NewWriter(nil))
	require.NoError(t, err)

	err = e.Close()
	assert.NoError(t, err)
}

// Decoder /////////////////////////////

func TestDoublePlainDecoder(t *testing.T) {
	t.Run("Init", TestDoublePlainDecoder_Init)
	t.Run("Init_NilReader", TestDoublePlainDecoder_Init_NilReader)
	t.Run("DecodeValues", TestDoublePlainDecoder_DecodeValues)
	t.Run("DecodeValues_ReadFail", TestDoublePlainDecoder_DecodeValues_ReadFail)
	t.Run("DecodeValues_NotEnoughValues", TestDoublePlainDecoder_DecodeValues_NotEnoughValues)
}

func TestDoublePlainDecoder_Init(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte("1234"))

	d := DoublePlainDecoder{}
	err := d.Init(reader)

	assert.NoError(t, err)
}

func TestDoublePlainDecoder_Init_NilReader(t *testing.T) {
	t.Parallel()

	d := DoublePlainDecoder{}
	err := d.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilReader.Error())
}

func TestDoublePlainDecoder_DecodeValues(t *testing.T) {
	t.Parallel()

	reader := memory.NewWriter([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 8, 0x40})

	d := DoublePlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	require.NoError(t, err)
	assert.Equal(t, 3, cnt)
	assert.ElementsMatch(t, []float64{1., 2., 3.}, dest)
}

func TestDoublePlainDecoder_DecodeValues_ReadFail(t *testing.T) {
	t.Parallel()

	reader := fakes.NewReaderMock(t)
	reader.ReadMock.Return(0, errors.New("read failed"))

	d := DoublePlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	assert.EqualError(t, errors.Cause(err), "read failed")
	assert.Equal(t, 0, cnt)
}

func TestDoublePlainDecoder_DecodeValues_NotEnoughValues(t *testing.T) {
	t.Parallel()

	reader := memory.NewWriter([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40})

	d := DoublePlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
	assert.Equal(t, 2, cnt)
	assert.ElementsMatch(t, []interface{}{1., 2., nil}, dest)
}
