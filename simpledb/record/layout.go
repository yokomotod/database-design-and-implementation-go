package record

import (
	"simpledb/file"
)

type Layout struct {
	schema   *Schema
	offset   map[string]int32
	slotSize int32
}

func NewLayoutFromSchema(schema *Schema) *Layout {
	offsets := make(map[string]int32)
	// empty/inuse flagのために整数分の領域(4byte)を確保
	pos := file.Int32Bytes
	for _, fieldName := range schema.Fields() {
		offsets[fieldName] = pos
		pos += lengthInBytes(schema, fieldName)
	}
	return NewLayout(schema, offsets, pos)
}

func NewLayout(schema *Schema, offsets map[string]int32, slotSize int32) *Layout {
	return &Layout{schema, offsets, slotSize}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int32 {
	return l.offset[fieldName]
}

func (l *Layout) SlotSize() int32 {
	return l.slotSize
}

func lengthInBytes(schema *Schema, fieldName string) int32 {
	fieldType := schema.Type(fieldName)
	if fieldType == INT {
		return file.Int32Bytes
	} else {
		return file.MaxLength(schema.Length(fieldName))
	}
}
