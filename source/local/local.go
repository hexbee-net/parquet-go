package local

import (
	"os"

	"github.com/hexbee-net/errors"
)

type File struct {
	FilePath string
	file     *os.File
}

// NewReader creates a local file Reader.
func NewReader(path string) (reader *File, err error) {
	reader = &File{
		FilePath: "",
		file:     nil,
	}

	if reader.file, err = os.Open(path); err != nil {
		return nil, errors.Wrap(err, "failed to open source file")
	}

	return reader, nil
}

// NewWriter creates an local file Writer.
func NewWriter(path string) (writer *File, err error) {
	writer = &File{
		FilePath: "",
		file:     nil,
	}

	if writer.file, err = os.Create(path); err != nil {
		return nil, errors.Wrap(err, "failed to create target file")
	}

	return writer, nil
}

// Reader //////////////////////////////

func (f *File) Read(b []byte) (cnt int, err error) {
	var n int

	ln := len(b)

	for cnt < ln {
		n, err = f.file.Read(b[cnt:])
		cnt += n

		if err != nil {
			break
		}
	}

	return cnt, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

// Writer //////////////////////////////

func (f *File) Write(p []byte) (n int, err error) {
	return f.file.Write(p)
}

func (f *File) Close() error {
	return f.file.Close()
}
