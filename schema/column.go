package schema

import (
	"github.com/hexbee-net/errors"
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
	return c.maxD
}

// MaxRepetitionLevel returns the maximum repetition value for this column.
func (c *Column) MaxRepetitionLevel() uint16 {
	return c.maxR
}

// FlatName returns the name of the column and its parents in dotted notation.
func (c *Column) FlatName() string {
	return c.flatName
}

// Name returns the column name.
func (c *Column) Name() string {
	return c.name
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
	if c.data == nil {
		return nil
	}

	return parquet.TypePtr(c.data.ParquetType())
}

// RepetitionType returns the repetition type for the current column.
func (c *Column) RepetitionType() *parquet.FieldRepetitionType {
	return &c.rep
}

// DataColumn returns true if the column is data column, false otherwise.
func (c *Column) IsDataColumn() bool {
	return c.data != nil
}

// ChildrenCount returns the number of children in a group.
func (c *Column) ChildrenCount() (int, error) {
	if c.data != nil {
		return 0, errors.New("not a group column")
	}

	return len(c.children), nil
}

func (c *Column) SetSkipped(b bool) {
	c.data.Skipped = b
}

func (c *Column) readGroupSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (newIndex int, err error) {
	if len(schema) <= idx {
		return 0, errors.WithFields(
			errors.New("schema index out of bound"),
			errors.Fields{
				"index": idx,
				"size":  len(schema),
			})
	}

	s := schema[idx]

	if s.Type != nil {
		return 0, errors.WithFields(
			errors.New("field type is not nil for group"),
			errors.Fields{
				"index": idx,
			})
	}

	if s.NumChildren == nil {
		return 0, errors.WithFields(
			errors.New("field NumChildren is invalid"),
			errors.Fields{
				"index": idx,
			})
	}

	if *s.NumChildren <= 0 {
		return 0, errors.WithFields(
			errors.New("field NumChildren is zero"),
			errors.Fields{
				"index": idx,
			})
	}

	l := int(*s.NumChildren)

	if len(schema) <= idx+l {
		return 0, errors.WithFields(
			errors.New("not enough element in schema list"),
			errors.Fields{
				"index": idx,
			})
	}

	if s.RepetitionType != nil && *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if s.RepetitionType != nil && *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	c.maxD = dLevel
	c.maxR = rLevel

	if name == "" {
		name = s.Name
	} else {
		name += "." + s.Name
	}

	c.flatName = name
	c.name = s.Name
	c.element = s
	c.children = make([]*Column, 0, l)
	c.rep = *s.RepetitionType

	idx++ // move idx from this group to next

	for i := 0; i < l; i++ {
		child := &Column{}

		if schema[idx].Type == nil {
			// another group
			idx, err = child.readGroupSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}

			c.children = append(c.children, child)
		} else {
			idx, err = child.readColumnSchema(schema, name, idx, dLevel, rLevel)
			if err != nil {
				return 0, err
			}

			c.children = append(c.children, child)
		}
	}

	return idx, nil
}

func (c *Column) readColumnSchema(schema []*parquet.SchemaElement, name string, idx int, dLevel, rLevel uint16) (newIndex int, err error) {
	s := schema[idx]

	if s.Name == "" {
		return 0, errors.WithFields(
			errors.New("name in schema is empty"),
			errors.Fields{
				"index": idx,
			})
	}

	if s.RepetitionType == nil {
		return 0, errors.WithFields(
			errors.New("field RepetitionType is nil"),
			errors.Fields{
				"index": idx,
			})
	}

	if *s.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
		dLevel++
	}

	if *s.RepetitionType == parquet.FieldRepetitionType_REPEATED {
		rLevel++
	}

	c.element = s
	c.maxR = rLevel
	c.maxD = dLevel
	c.rep = *s.RepetitionType
	c.name = s.Name

	if name == "" {
		c.flatName = s.Name
	} else {
		c.flatName = name + "." + s.Name
	}

	c.data, err = datastore.GetValuesStore(s)
	if err != nil {
		return 0, err
	}

	return idx + 1, nil
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
