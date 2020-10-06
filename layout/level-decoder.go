package layout

import "github.com/hexbee-net/parquet/encoding"

type levelDecoder interface {
	encoding.Decoder

	maxLevel() uint16
}

type levelDecoderWrapper struct {
	encoding.Decoder
	max uint16
}

func (l *levelDecoderWrapper) maxLevel() uint16 {
	return l.max
}
