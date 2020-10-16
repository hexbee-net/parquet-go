package source

import "io"

type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
}
