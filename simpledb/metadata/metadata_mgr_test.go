package metadata_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/metadata"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestMetadataMgr(t *testing.T) {
	simpleDB, err := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	tx := simpleDB.NewTx()
	mdm, err := metadata.NewMetadataMgr(true, tx)
	if err != nil {
		t.Fatalf("failed to create MetadataMgr: %v", err)
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
	tableScan, err := record.NewTableScan(tx, "MyTable", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	for i := 0; i < 50; i++ {
		tableScan.Insert()
		n := rand.Int31n(50)
		tableScan.SetInt("A", n)
		tableScan.SetString("B", fmt.Sprintf("rec%d", n))
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
	viewdef := "select B from MyTable where A = 1"
	err = mdm.CreateView("viewA", viewdef, tx)
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
	idxmap, err := mdm.GetIndexInfo("MyTable", tx)
	if err != nil {
		t.Fatalf("failed to get IndexInfo: %v", err)
	}
	ii := idxmap["A"]
	// TODO: HashIndex が実装されたら確認できる
	// fmt.Printf("B(indexA) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexA) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexA,A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexA,B) = %d\n", ii.DistinctValues("B"))

	ii = idxmap["B"]
	// TODO: HashIndex が実装されたら確認できる
	// fmt.Printf("B(indexB) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexB) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexB,A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexB,B) = %d\n", ii.DistinctValues("B"))

	tx.Commit()
}