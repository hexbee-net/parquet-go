// +build gofuzz

package encoding

import "bytes"

func FuzzDeltaBinaryPackDecoder32(data []byte) int {
	d := DeltaBinaryPackDecoder32{}

	if err := d.Init(bytes.NewReader(data)); err != nil {
		return 0
	}

	for i := 0; i < len(data)/4; i++ {
		if _, err := d.Next(); err != nil {
			return 0
		}
	}

	return 1
}

func FuzzDeltaBinaryPackDecoder64(data []byte) int {
	d := DeltaBinaryPackDecoder64{}

	if err := d.Init(bytes.NewReader(data)); err != nil {
		return 0
	}

	for i := 0; i < len(data)/4; i++ {
		if _, err := d.Next(); err != nil {
			return 0
		}
	}

	return 1
}
