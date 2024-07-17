package record_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestTableScan(t *testing.T) {
	simpleDB, err := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	transaction := simpleDB.NewTx()
	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(schema)
	for _, fieldName := range schema.Fields() {
		offset := layout.Offset(fieldName)
		fmt.Printf("%s has offset %d\n", fieldName, offset)
	}
	fmt.Println("Filling the table with 50 random records.")
	tableScan, err := record.NewTableScan(transaction, "T", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	for i := 0; i < 50; i++ {
		tableScan.Insert()
		n := rand.Int31n(50)
		tableScan.SetInt("A", n)
		tableScan.SetString("B", fmt.Sprintf("rec%d", n))
		fmt.Printf("inserting record into slot %s: {%d, %s}\n", tableScan.GetRID().String(), n, fmt.Sprintf("rec%d", n))
	}
	fmt.Println("Deleting these records, whose A-values are less than 25.")
	count := 0
	tableScan.BeforeFirst()
	next, err := tableScan.Next()
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	for next {
		a, err := tableScan.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := tableScan.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		if a < 25 {
			count++
			fmt.Printf("deleting record from slot %s: {%d, %s}\n", tableScan.GetRID().String(), a, b)
			tableScan.Delete()
		}
		next, err = tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
	}
	fmt.Printf("%d values under 25 were deleted\n", count)

	fmt.Println("Here are the remaining records.")
	tableScan.BeforeFirst()
	next, err = tableScan.Next()
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	for next {
		a, err := tableScan.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := tableScan.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		fmt.Printf("slot %s: {%d, %s}\n", tableScan.GetRID().String(), a, b)
		next, err = tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
	}
	tableScan.Close()
	transaction.Commit()
}
