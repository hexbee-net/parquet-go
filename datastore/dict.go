package datastore

import (
	"hash/fnv"

	"github.com/hexbee-net/errors"
)

type DictStore struct {
	Values     []interface{}
	Data       []int32
	indices    map[interface{}]int32
	size       int64
	valueSize  int64
	readPos    int
	nullCount  int32
	NoDictMode bool

	hashFunc func([]byte) interface{}
}

func (s *DictStore) Init() {
	s.Values = s.Values[:0]
	s.Data = s.Data[:0]
	s.indices = make(map[interface{}]int32)
	s.size = 0
	s.readPos = 0
	s.nullCount = 0

	s.hashFunc = fnvHashFunc
}

func (s *DictStore) GetNextValue() (interface{}, error) {
	if s.NoDictMode {
		if s.readPos >= len(s.Values) {
			return nil, errors.New("out of range")
		}

		s.readPos++

		return s.Values[s.readPos-1], nil
	}

	if s.readPos >= len(s.Data) {
		return nil, errors.New("out of range")
	}

	s.readPos++

	pos := s.Data[s.readPos-1]

	return s.Values[pos], nil
}

func (s *DictStore) AddValue(v interface{}, size int) {
	if v == nil {
		s.nullCount++
		return
	}

	s.size += int64(size)
	s.Data = append(s.Data, s.getIndex(v, size))
}

func (s *DictStore) NumValues() int32 {
	return int32(len(s.Data))
}

func (s *DictStore) getIndex(in interface{}, size int) int32 {
	key := s.mapKey(in)

	if idx, ok := s.indices[key]; ok {
		return idx
	}

	s.valueSize += int64(size)
	s.Values = append(s.Values, in)
	idx := int32(len(s.Values) - 1)

	s.indices[key] = idx

	return idx
}

func (s *DictStore) mapKey(in interface{}) interface{} {
	switch v := in.(type) {
	case int, int32, int64, string, bool, float64, float32:
		return in
	case []byte:
		return s.hashFunc(v)
	case [12]byte:
		return s.hashFunc(v[:])
	default:
		panic("not supported type")
	}
}

func fnvHashFunc(in []byte) interface{} {
	hash := fnv.New64()
	if err := writeFull(hash, in); err != nil {
		panic(err)
	}
	return hash.Sum64()
}
