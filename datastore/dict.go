package datastore

type Dict struct {
	Values     []interface{}
	data       []int32
	indices    map[interface{}]int32
	size       int64
	valueSize  int64
	readPos    int
	nullCount  int32
	noDictMode bool
}
