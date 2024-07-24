package metadata_test

import (
	"fmt"
	"path"
	"simpledb/metadata"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestTableMgr(t *testing.T) {
	simpleDB, err := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	transaction := simpleDB.NewTx()
	tableMgr := metadata.NewTableMgr(true, transaction)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	err = tableMgr.CreateTable("MyTable", schema, transaction)
	if err != nil {
		t.Fatalf("failed to create MyTable: %v", err)
	}

	layout, err := tableMgr.GetLayout("MyTable", transaction)
	if err != nil {
		t.Fatalf("failed to GetLayout: %v", err)
	}
	size := layout.SlotSize()
	schema2 := layout.Schema()
	fmt.Printf("MyTable has slot size %d\n", size)
	fmt.Println("Its fields are:")
	for _, fieldName := range schema2.Fields() {
		fieldType := ""
		if schema.Type(fieldName) == record.INT {
			fieldType = "int"
		} else {
			strlen := schema2.Length(fieldName)
			fieldType = fmt.Sprintf("varchar(%d)", strlen)
		}
		fmt.Printf("%s: %s\n", fieldName, fieldType)
	}
	transaction.Commit()
}
