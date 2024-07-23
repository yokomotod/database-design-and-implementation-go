package record_test

import (
	"simpledb/record"
	"testing"
)

func TestLayout(t *testing.T) {
	t.Parallel()
	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(schema)
	if layout.Offset("A") != 4 {
		t.Errorf("A offset %d", layout.Offset("A"))
	}
	if layout.Offset("B") != 8 {
		t.Errorf("B offset %d", layout.Offset("B"))
	}
}
