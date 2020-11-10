package types

import (
	"io"
	"math/bits"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/datastore"
	"github.com/hexbee-net/parquet/encoding"
)

// Encoder /////////////////////////////

type DictEncoder struct {
	datastore.DictStore
	writer io.Writer
}

func (e *DictEncoder) Init(writer io.Writer) error {
	e.writer = writer
	e.DictStore.Init()

	return nil
}

func (e *DictEncoder) EncodeValues(values []interface{}) error {
	for i := range values {
		e.AddValue(values[i], 0) // size is not important here
	}

	return nil
}

func (e *DictEncoder) Close() error {
	v := len(e.Values)
	if v == 0 { // empty dictionary?
		return errors.New("empty dictionary nothing to write")
	}

	w := bits.Len(uint(v))

	// first write the bitLength in a byte
	if err := writeFull(e.writer, []byte{byte(w)}); err != nil {
		return err
	}

	enc, err := encoding.NewHybridEncoder(w)
	if err != nil {
		return err
	}

	if err = enc.Append(e.Data); err != nil {
		return err
	}

	return enc.Write(e.writer)
}

// Decoder /////////////////////////////

type DictDecoder struct {
	Values []interface{}
	keys   encoding.Decoder
}

func (d *DictDecoder) Init(reader io.Reader) error {
	buf := make([]byte, 1)

	if _, err := io.ReadFull(reader, buf); err != nil {
		return errors.WithStack(err)
	}

	w := int(buf[0])

	if w < 0 || w > 32 {
		return errors.WithFields(
			errors.New("invalid bit-width"),
			errors.Fields{
				"bit-width": w,
			})
	}

	if w >= 0 {
		d.keys = encoding.NewHybridDecoder(w, false)
		return d.keys.Init(reader)
	}

	return errors.New("bit-width zero with non-empty dictionary")
}

func (d *DictDecoder) DecodeValues(dest []interface{}) (count int, err error) {
	if d.keys == nil {
		return 0, errors.New("no value is inside dictionary")
	}

	size := int32(len(d.Values))

	for i := range dest {
		key, err := d.keys.Next()
		if err != nil {
			return i, err
		}

		if key >= size {
			return 0, errors.WithFields(
				errors.New("invalid index"),
				errors.Fields{
					"index":        key,
					"values-count": size,
				})
		}

		dest[i] = d.Values[key]
	}

	return len(dest), nil
}
