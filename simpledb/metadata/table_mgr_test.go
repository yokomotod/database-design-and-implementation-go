package metadata_test

import (
	"fmt"
	"path"
	"simpledb/metadata"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestTableManager(t *testing.T) {
	simpleDB, err := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	transaction, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}
	tableManager, err := metadata.NewTableManager(true, transaction)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	err = tableManager.CreateTable("MyTable", schema, transaction)
	if err != nil {
		t.Fatalf("failed to create MyTable: %v", err)
	}

	layout, err := tableManager.GetLayout("MyTable", transaction)
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
	if err := transaction.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
