package datastore

import (
	"math/bits"

	"github.com/hexbee-net/errors"
	"github.com/hexbee-net/parquet/encoding"
	"github.com/hexbee-net/parquet/parquet"
)

// parquetColumn is to convert a store to a parquet.SchemaElement.
type parquetColumn interface {
	ParquetType() parquet.Type
	RepetitionType() parquet.FieldRepetitionType
	Params() (*ColumnParameters, error)
}

type typedColumnStore interface {
	parquetColumn

	Reset(repetitionType parquet.FieldRepetitionType)

	// Min and Max in parquet byte
	MaxValue() []byte
	MinValue() []byte

	// Should extract the value, turn it into an array and check for min and max on all values in this
	GetValues(v interface{}) ([]interface{}, error)
	SizeOf(v interface{}) int

	// the tricky append. this is a way of creating new "typed" array. the first interface is nil or an []T (T is the type,
	// not the interface) and value is from that type. the result should be always []T (array of that type)
	// exactly like the builtin append
	Append(arrayIn, value interface{}) interface{}
}

// ColumnStore is the read/write implementation for a column.
// It buffers a single column's data that is to be written to a parquet file,
// knows how to encode this data and will choose an optimal way according to
// heuristics.
// It also ensures the correct decoding of column data to be read.
type ColumnStore struct {
	typedColumnStore

	repTyp parquet.FieldRepetitionType

	Values *DictStore

	DefinitionLevels *encoding.PackedArray
	RepetitionLevels *encoding.PackedArray

	encoding parquet.Encoding
	readPos  int

	allowDict bool

	Skipped bool
}

func NewColumnStore(typed typedColumnStore, enc parquet.Encoding, allowDict bool) *ColumnStore {
	return &ColumnStore{
		typedColumnStore: typed,
		encoding:         enc,
		allowDict:        allowDict,
	}
}

func newPlainColumnStore(typed typedColumnStore) *ColumnStore {
	return NewColumnStore(typed, parquet.Encoding_PLAIN, true)
}

func (s *ColumnStore) Reset(rep parquet.FieldRepetitionType, maxR, maxD uint16) error {
	if s.typedColumnStore == nil {
		panic("generic should be used with typed column store")
	}

	s.repTyp = rep

	if s.Values == nil {
		s.Values = &DictStore{}
		s.RepetitionLevels = &encoding.PackedArray{}
		s.DefinitionLevels = &encoding.PackedArray{}
	}

	s.Values.Init()

	if err := s.RepetitionLevels.Reset(bits.Len16(maxR)); err != nil {
		return err
	}

	if err := s.DefinitionLevels.Reset(bits.Len16(maxD)); err != nil {
		return err
	}

	s.readPos = 0
	s.Skipped = false

	s.typedColumnStore.Reset(rep)

	return nil
}

func (s *ColumnStore) GetRDLevelAt(pos int) (rLevel, dLevel int32, last bool) {
	var err error

	if pos < 0 {
		pos = s.readPos
	}

	if pos >= s.RepetitionLevels.Count() || pos >= s.DefinitionLevels.Count() {
		return 0, 0, true
	}

	dLevel, err = s.DefinitionLevels.At(pos)
	if err != nil {
		return 0, 0, true
	}

	rLevel, err = s.RepetitionLevels.At(pos)
	if err != nil {
		return 0, 0, true
	}

	return rLevel, dLevel, false
}

func (s *ColumnStore) Get(maxD, maxR int32) (value interface{}, maxDefinitions int32, err error) {
	if s.Skipped {
		return nil, 0, nil
	}

	if s.readPos >= s.RepetitionLevels.Count() || s.readPos >= s.DefinitionLevels.Count() {
		return nil, 0, errors.New("out of range")
	}

	_, dl, _ := s.GetRDLevelAt(s.readPos)
	// this is a null value, increase the read pos, for advancing the rLvl and dLvl but
	// do not touch the dict-store
	if dl < maxD {
		s.readPos++
		return nil, dl, nil
	}

	v, err := s.GetNext()
	if err != nil {
		return nil, 0, err
	}

	// if this is not repeated just return the value, the result is not an array
	if s.repTyp != parquet.FieldRepetitionType_REPEATED {
		s.readPos++
		return v, maxD, err
	}

	// the first rLevel in current object is always less than maxR (only for the repeated values)
	// the next data in this object, should have maxR as the rLevel. the first rLevel less than maxR means the value
	// is from the next object and we should not touch it in this call

	var ret = s.typedColumnStore.Append(nil, v)

	for {
		s.readPos++

		rl, _, last := s.GetRDLevelAt(s.readPos)
		if last || rl < maxR {
			// end of this object
			return ret, maxD, nil
		}

		v, err := s.GetNext()
		if err != nil {
			return nil, maxD, err
		}

		ret = s.typedColumnStore.Append(ret, v)
	}
}

func (s *ColumnStore) GetNext() (v interface{}, err error) {
	v, err = s.Values.GetNextValue()
	if err != nil {
		return nil, err
	}

	return v, nil
}
