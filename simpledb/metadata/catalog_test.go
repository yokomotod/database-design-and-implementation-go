package metadata_test

import (
	"fmt"
	"path"
	"simpledb/metadata"
	"simpledb/query"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestCatalog(t *testing.T) {
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

	fmt.Println("All tables and their lengths:")
	layout, err := tableManager.GetLayout("tblcat", transaction)
	if err != nil {
		t.Fatalf("failed to tblcat layout: %v", err)
	}
	tableScan, err := query.NewTableScan(transaction, "tblcat", layout)
	if err != nil {
		t.Fatalf("failed to create tblcat table scan: %v", err)
	}
	for {
		next, err := tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get tblcat next: %v", err)
		}
		if !next {
			break
		}
		tableName, err := tableScan.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get tblname: %v", err)
		}
		size, err := tableScan.GetInt("slotsize")
		if err != nil {
			t.Fatalf("failed to get slotsize: %v", err)
		}
		fmt.Printf("%s %d\n", tableName, size)
	}
	tableScan.Close()

	fmt.Println("All fields and their offsets:")
	layout, err = tableManager.GetLayout("fldcat", transaction)
	if err != nil {
		t.Fatalf("failed to fldcat layout: %v", err)
	}
	tableScan, err = query.NewTableScan(transaction, "fldcat", layout)
	if err != nil {
		t.Fatalf("failed to create fldcat table scan: %v", err)
	}
	for {
		next, err := tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get fldcat next: %v", err)
		}
		if !next {
			break
		}
		tableName, err := tableScan.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get tblname: %v", err)
		}
		fieldName, err := tableScan.GetString("fldname")
		if err != nil {
			t.Fatalf("failed to get fldname: %v", err)
		}
		offset, err := tableScan.GetInt("offset")
		if err != nil {
			t.Fatalf("failed to get offset: %v", err)
		}
		fmt.Printf("%s %s %d\n", tableName, fieldName, offset)
	}
	tableScan.Close()
	if err := transaction.Commit(); err != nil { // 紙面上だと書かれていないが、ないと実行が終わらないはず
		t.Fatalf("failed to commit: %v", err)
	}
}
