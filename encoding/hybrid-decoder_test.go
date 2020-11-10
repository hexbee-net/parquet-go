package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

func TestHybridDecoder_GroupBoundary(t *testing.T) {
	b := []byte{
		(1 << 1) | 1,
		(1 << 0) | (2 << 2) | (3 << 4),
	}

	d := NewHybridDecoder(2, false)

	reader := bytes.NewReader(b)
	require.NoError(t, d.Init(reader))

	v, err := d.Next()
	assert.Equal(t, int32(1), v)
	assert.NoError(t, err)

	v, err = d.Next()
	assert.Equal(t, int32(2), v)
	assert.NoError(t, err)

	v, err = d.Next()
	assert.Equal(t, int32(3), v)
	assert.NoError(t, err)

	assert.Equal(t, 0, reader.Len())
}
