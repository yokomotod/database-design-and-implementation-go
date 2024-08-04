package metadata_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/query"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestMetadataManager(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "tabletest"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	mdm := simpleDB.MetadataManager()
	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	// Part 1: Table Metadata
	err = mdm.CreateTable("MyTable", schema, tx)
	if err != nil {
		t.Fatalf("failed to create MyTable: %v", err)
	}
	layout, err := mdm.GetLayout("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get layout: %v", err)
	}
	size := layout.SlotSize()
	schema2 := layout.Schema()
	fmt.Printf("MyTable has slot size %d\n", size)
	fmt.Println("Its fields are:")
	for _, fieldName := range schema2.Fields() {
		fieldType := ""
		if schema2.Type(fieldName) == record.INT {
			fieldType = "int"
		} else {
			strlen := schema2.Length(fieldName)
			fieldType = fmt.Sprintf("varchar(%d)", strlen)
		}
		fmt.Printf("%s: %s\n", fieldName, fieldType)
	}

	// Part 2: Statistics Metadata
	tableScan, err := query.NewTableScan(tx, "MyTable", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := tableScan.Insert(); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		n := rand.Int31n(50)
		if err := tableScan.SetInt("A", n); err != nil {
			t.Fatalf("failed to set int: %v", err)
		}
		if err := tableScan.SetString("B", fmt.Sprintf("rec%d", n)); err != nil {
			t.Fatalf("failed to set string: %v", err)
		}
	}
	si, err := mdm.GetStatInfo("MyTable", layout, tx)
	if err != nil {
		t.Fatalf("failed to get StatInfo: %v", err)
	}
	fmt.Printf("B(MyTable) = %d\n", si.BlocksAccessed())
	fmt.Printf("R(MyTable) = %d\n", si.RecordsOutput())
	fmt.Printf("V(MyTable,A) = %d\n", si.DistinctValues("A"))
	fmt.Printf("V(MyTable,B) = %d\n", si.DistinctValues("B"))

	// Part 3: View Metadata
	viewDef := "select B from MyTable where A = 1"
	err = mdm.CreateView("viewA", viewDef, tx)
	if err != nil {
		t.Fatalf("failed to create view: %v", err)
	}
	v, err := mdm.GetViewDef("viewA", tx)
	if err != nil {
		t.Fatalf("failed to get view definition: %v", err)
	}
	fmt.Printf("View def = %s\n", v)

	// Part 4: Index Metadata
	err = mdm.CreateIndex("indexA", "MyTable", "A", tx)
	if err != nil {
		t.Fatalf("failed to create indexA: %v", err)
	}
	err = mdm.CreateIndex("indexB", "MyTable", "B", tx)
	if err != nil {
		t.Fatalf("failed to create indexB: %v", err)
	}
	indexMap, err := mdm.GetIndexInfo("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get IndexInfo: %v", err)
	}
	ii := indexMap["A"]
	// TODO: HashIndex が実装されたら確認できる
	// fmt.Printf("B(indexA) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexA) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexA,A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexA,B) = %d\n", ii.DistinctValues("B"))

	ii = indexMap["B"]
	// TODO: HashIndex が実装されたら確認できる
	// fmt.Printf("B(indexB) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexB) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexB,A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexB,B) = %d\n", ii.DistinctValues("B"))

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
