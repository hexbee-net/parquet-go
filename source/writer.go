package source

import "io"

type Writer interface {
	io.Writer
	io.Closer

	Create(name string) (Writer, error)
}
