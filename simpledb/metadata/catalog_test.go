package metadata_test

import (
	"fmt"
	"path"
	"simpledb/metadata"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestCatalog(t *testing.T) {
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

	fmt.Println("All tables and their lengths:")
	layout, err := tableMgr.GetLayout("tblcat", transaction)
	if err != nil {
		t.Fatalf("failed to tblcat layout: %v", err)
	}
	tableScan, err := record.NewTableScan(transaction, "tblcat", layout)
	if err != nil {
		t.Fatalf("failed to create tblcat table scan: %v", err)
	}
	next, err := tableScan.Next()
	if err != nil {
		t.Fatalf("failed to get tblcat next: %v", err)
	}
	for next {
		tname, err := tableScan.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get tblname: %v", err)
		}
		size, err := tableScan.GetInt("slotsize")
		if err != nil {
			t.Fatalf("failed to get slotsize: %v", err)
		}
		fmt.Printf("%s %d\n", tname, size)
		next, err = tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get tblcat next: %v", err)
		}
	}
	tableScan.Close()

	fmt.Println("All fields and their offsets:")
	layout, err = tableMgr.GetLayout("fldcat", transaction)
	if err != nil {
		t.Fatalf("failed to fldcat layout: %v", err)
	}
	tableScan, err = record.NewTableScan(transaction, "fldcat", layout)
	if err != nil {
		t.Fatalf("failed to create fldcat table scan: %v", err)
	}
	next, err = tableScan.Next()
	if err != nil {
		t.Fatalf("failed to get fldcat next: %v", err)
	}
	for next {
		tname, err := tableScan.GetString("tblname")
		if err != nil {
			t.Fatalf("failed to get tblname: %v", err)
		}
		fname, err := tableScan.GetString("fldname")
		if err != nil {
			t.Fatalf("failed to get fldname: %v", err)
		}
		offset, err := tableScan.GetInt("offset")
		if err != nil {
			t.Fatalf("failed to get offset: %v", err)
		}
		fmt.Printf("%s %s %d\n", tname, fname, offset)
		next, err = tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get fldcat next: %v", err)
		}
	}
	tableScan.Close()
	transaction.Commit() // 紙面上だと書かれていないが、ないと実行が終わらないはず
}