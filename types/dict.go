package types

import (
	"io"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
)

// Encoder /////////////////////////////

type DictEncoder struct {
	//dictStore
	w io.Writer
}

func (e *DictEncoder) Close() error {
	panic("implement me")
}

func (e *DictEncoder) Init(writer io.Writer) error {
	panic("implement me")
}

func (e *DictEncoder) EncodeValues(values []interface{}) error {
	panic("implement me")
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
		d.keys = encoding.NewHybridDecoder(w)
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
