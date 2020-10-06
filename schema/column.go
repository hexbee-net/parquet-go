package schema

import (
	"github.com/hexbee-net/parquet/datastore"
	"github.com/hexbee-net/parquet/parquet"
	"github.com/hexbee-net/parquet/schema/definition"
)

// ColumnParameters contains common parameters related to a column.
type ColumnParameters struct {
	LogicalType   *parquet.LogicalType
	ConvertedType *parquet.ConvertedType
	TypeLength    *int32
	FieldID       *int32
	Scale         *int32
	Precision     *int32
}

type Column struct {
	index    int
	name     string
	flatName string

	nameArray []string

	// one of the following should be not null. data or children
	data     *datastore.ColumnStore
	children []*Column

	rep parquet.FieldRepetitionType

	maxR uint16
	maxD uint16

	parent int // one of noParent, listParent, mapParent

	// for the reader we should read this element from the meta, for the writer we need to build this element
	element *parquet.SchemaElement

	params *ColumnParameters
}

// AsColumnDefinition creates a new column definition from the provided column.
func (c *Column) AsColumnDefinition() *definition.Column {
	col := &definition.Column{
		SchemaElement: c.Element(),
	}

	for _, child := range c.Children() {
		col.Children = append(col.Children, child.AsColumnDefinition())
	}

	return col
}

// Children returns the column's child columns.
func (c *Column) Children() []*Column {
	return c.children
}

// ColumnStore returns the underlying column store.
func (c *Column) ColumnStore() *datastore.ColumnStore {
	return c.data
}

// MaxDefinitionLevel returns the maximum definition level for this column.
func (c *Column) MaxDefinitionLevel() uint16 {
	panic("implement me")
}

// MaxRepetitionLevel returns the maximum repetition value for this column.
func (c *Column) MaxRepetitionLevel() uint16 {
	panic("implement me")
}

// FlatName returns the name of the column and its parents in dotted notation.
func (c *Column) FlatName() string {
	panic("implement me")
}

// Name returns the column name.
func (c *Column) Name() string {
	panic("implement me")
}

// Index returns the index of the column in schema, zero based.
func (c *Column) Index() int {
	return c.index
}

// Element returns schema element definition of the column.
func (c *Column) Element() *parquet.SchemaElement {
	if c.element == nil {
		// If this is a no-element node, we need to re-create element every time to make sure the content is always up-to-date
		return c.buildElement()
	}

	return c.element
}

// Type returns the parquet type of the value.
// Returns nil if the column is a group.
func (c *Column) Type() *parquet.Type {
	panic("implement me")
}

// RepetitionType returns the repetition type for the current column.
func (c *Column) RepetitionType() *parquet.FieldRepetitionType {
	panic("implement me")
}

// DataColumn returns true if the column is data column, false otherwise.
func (c *Column) IsDataColumn() bool {
	panic("implement me")
}

// ChildrenCount returns the number of children in a group.
// Returns -1 if the column is a data column.
func (c *Column) ChildrenCount() int {
	panic("implement me")
}

func (c *Column) SetSkipped(b bool) {
	// c.data.skipped = b
	panic("implement me")
}

func (c *Column) readGroupSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel int, rLevel int) (int, error) {
	panic("implement me")
}

func (c *Column) readColumnSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel int, rLevel int) (int, error) {
	panic("implement me")
}

func (c *Column) buildElement() *parquet.SchemaElement {
	rep := c.rep
	elem := &parquet.SchemaElement{
		RepetitionType: &rep,
		Name:           c.name,
	}

	if c.params != nil {
		elem.FieldID = c.params.FieldID
		elem.ConvertedType = c.params.ConvertedType
		elem.LogicalType = c.params.LogicalType
	}

	if c.data != nil {
		elem.Type = parquet.TypePtr(c.data.ParquetType())
		elem.TypeLength = c.params.TypeLength
		elem.Scale = c.params.Scale
		elem.Precision = c.params.Precision
	} else {
		nc := int32(len(c.children))
		elem.NumChildren = &nc
	}

	return elem
}
