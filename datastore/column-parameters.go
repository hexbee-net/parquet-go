package datastore

import "github.com/hexbee-net/parquet/parquet"

// ColumnParameters contains common parameters related to a column.
type ColumnParameters struct {
	LogicalType   *parquet.LogicalType
	ConvertedType *parquet.ConvertedType
	TypeLength    *int32
	FieldID       *int32
	Scale         *int32
	Precision     *int32
}
