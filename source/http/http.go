package http

import (
	"mime/multipart"

	"github.com/hexbee-net/errors"
)

type Reader struct {
	fileHeader *multipart.FileHeader
	file       multipart.File
}

func NewReader(header *multipart.FileHeader, file multipart.File) (r *Reader, err error) {
	r = &Reader{
		fileHeader: header,
		file:       file,
	}

	r.file, err = r.fileHeader.Open()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open HTTP stream")
	}

	return r, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.file.Read(p)
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.file.Seek(offset, whence)
}

func (r *Reader) Close() error {
	return r.file.Close()
}
