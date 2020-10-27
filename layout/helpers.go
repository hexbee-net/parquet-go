package layout

import (
	"io"
	"math/bits"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
)

type thriftReader interface {
	Read(thrift.TProtocol) error
}

func readThrift(tr thriftReader, r io.Reader) error {
	// Make sure we are not using any kind of buffered reader here.
	// bufio.Reader "can" reads more data ahead of time, which is a problem on this library
	transport := &thrift.StreamTransport{Reader: r}
	proto := thrift.NewTCompactProtocol(transport)

	return tr.Read(proto)
}

type thriftWriter interface {
	Write(thrift.TProtocol) error
}

func writeThrift(tr thriftWriter, w io.Writer) error {
	transport := &thrift.StreamTransport{Writer: w}
	proto := thrift.NewTCompactProtocol(transport)

	return tr.Write(proto)
}

// /////////////////////////////////////////////////////////////////////////////

type offsetReader struct {
	inner  io.ReadSeeker
	offset int64
	count  int64
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	r.offset += int64(n)
	r.count += int64(n)

	return n, err
}

func (r *offsetReader) Seek(offset int64, whence int) (int64, error) {
	i, err := r.inner.Seek(offset, whence)
	if err == nil {
		r.count += i - r.offset
		r.offset = i
	}

	return i, err
}

func (r *offsetReader) Count() int64 {
	return r.count
}

// /////////////////////////////////////////////////////////////////////////////

func decodePackedArray(d levelDecoder, count int) (*encoding.PackedArray, int, error) {
	array := &encoding.PackedArray{}
	notNull := 0 // Counting not nulls only good for dLevels

	if err := array.Reset(bits.Len16(d.maxLevel())); err != nil {
		return nil, 0, err
	}

	for i := 0; i < count; i++ {
		u, err := d.Next()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}

		array.AppendSingle(u)

		if u == int32(d.maxLevel()) {
			notNull++
		}
	}

	return array, notNull, nil
}
