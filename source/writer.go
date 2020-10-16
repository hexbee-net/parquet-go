package source

import "io"

type Writer interface {
	io.Writer
	io.Closer
}
