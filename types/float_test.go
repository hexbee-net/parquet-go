package types

import (
	"bytes"
	"io"
	"testing"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/source/memory"
	"github.com/hexbee-net/parquet/tests/fakes"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

// Encoder /////////////////////////////

func TestFloatPlainEncoder(t *testing.T) {
	t.Run("Init", TestFloatPlainEncoder_Init)
	t.Run("Init_NilWriter", TestFloatPlainEncoder_Init_NilWriter)
	t.Run("EncodeValues", TestFloatPlainEncoder_EncodeValues)
	t.Run("EncodeValues_WrongType", TestFloatPlainEncoder_EncodeValues_WrongType)
	t.Run("Close", TestFloatPlainEncoder_Close)
}

func TestFloatPlainEncoder_Init(t *testing.T) {
	t.Parallel()

	e := FloatPlainEncoder{}

	err := e.Init(memory.NewWriter(nil))

	assert.NoError(t, err)
}

func TestFloatPlainEncoder_Init_NilWriter(t *testing.T) {
	t.Parallel()

	e := FloatPlainEncoder{}

	err := e.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilWriter.Error())
}

func TestFloatPlainEncoder_EncodeValues(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)

	e := FloatPlainEncoder{}
	err := e.Init(writer)
	require.NoError(t, err)

	values := []interface{}{float32(1.), float32(2.), float32(3.)}
	err = e.EncodeValues(values)
	require.NoError(t, err)

	err = e.Close()
	require.NoError(t, err)

	assert.ElementsMatch(t, []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40}, writer.Bytes())
}

func TestFloatPlainEncoder_EncodeValues_WrongType(t *testing.T) {
	t.Parallel()

	writer := memory.NewWriter(nil)

	e := FloatPlainEncoder{}
	err := e.Init(writer)
	require.NoError(t, err)

	values := []interface{}{true, false, true}
	err = e.EncodeValues(values)

	assert.EqualError(t, errors.Cause(err), errInvalidType.Error())
}

func TestFloatPlainEncoder_Close(t *testing.T) {
	t.Parallel()

	e := FloatPlainEncoder{}
	err := e.Init(memory.NewWriter(nil))
	require.NoError(t, err)

	err = e.Close()
	assert.NoError(t, err)
}

// Decoder /////////////////////////////

func TestFloatPlainDecoder(t *testing.T) {
	t.Run("Init", TestFloatPlainDecoder_Init)
	t.Run("Init_NilReader", TestFloatPlainDecoder_Init_NilReader)
	t.Run("DecodeValues", TestFloatPlainDecoder_DecodeValues)
	t.Run("DecodeValues_ReadFail", TestFloatPlainDecoder_DecodeValues_ReadFail)
	t.Run("DecodeValues_NotEnoughValues", TestFloatPlainDecoder_DecodeValues_NotEnoughValues)
}

func TestFloatPlainDecoder_Init(t *testing.T) {
	t.Parallel()

	reader := bytes.NewReader([]byte("1234"))

	d := FloatPlainDecoder{}
	err := d.Init(reader)

	assert.NoError(t, err)
}

func TestFloatPlainDecoder_Init_NilReader(t *testing.T) {
	t.Parallel()

	d := FloatPlainDecoder{}
	err := d.Init(nil)

	assert.EqualError(t, errors.Cause(err), errNilReader.Error())
}

func TestFloatPlainDecoder_DecodeValues(t *testing.T) {
	t.Parallel()

	reader := memory.NewWriter([]byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40})

	d := FloatPlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	require.NoError(t, err)
	assert.Equal(t, 3, cnt)
	assert.ElementsMatch(t, []float32{1., 2., 3.}, dest)
}

func TestFloatPlainDecoder_DecodeValues_ReadFail(t *testing.T) {
	t.Parallel()

	reader := fakes.NewReaderMock(t)
	reader.ReadMock.Return(0, errors.New("read failed"))

	d := FloatPlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	assert.EqualError(t, errors.Cause(err), "read failed")
	assert.Equal(t, 0, cnt)
}

func TestFloatPlainDecoder_DecodeValues_NotEnoughValues(t *testing.T) {
	t.Parallel()

	reader := memory.NewWriter([]byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40})

	d := FloatPlainDecoder{}
	err := d.Init(reader)
	require.NoError(t, err)

	dest := make([]interface{}, 3)
	cnt, err := d.DecodeValues(dest)

	assert.EqualError(t, errors.Cause(err), io.EOF.Error())
	assert.Equal(t, 2, cnt)
	assert.ElementsMatch(t, []interface{}{float32(1.), float32(2.), nil}, dest)
}
