package source

import "io"

type Reader interface {
	io.Reader
	io.Seeker

	Open(name string) (Reader, error)
}
