package memory

import (
	"bytes"
)

type Writer struct {
	bytes.Buffer
}

func NewWriter(buf []byte) *Writer {
	return &Writer{
		Buffer: *bytes.NewBuffer(buf),
	}
}

func (w Writer) Close() error {
	return nil
}
