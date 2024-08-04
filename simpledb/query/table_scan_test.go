package query_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/query"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestTableScan(t *testing.T) {
	t.Parallel()
	simpleDB, err := server.NewSimpleDB(path.Join(t.TempDir(), "tabletest"), 400, 8)
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	transaction, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}
	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(schema)
	for _, fieldName := range schema.Fields() {
		offset := layout.Offset(fieldName)
		fmt.Printf("%s has offset %d\n", fieldName, offset)
	}
	fmt.Println("Filling the table with 50 random records.")
	tableScan, err := query.NewTableScan(transaction, "T", layout)
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
		rid, err := tableScan.GetRID()
		if err != nil {
			t.Fatalf("failed to get rid: %v", err)
		}
		fmt.Printf("inserting record into slot %s: {%d, %s}\n", rid.String(), n, fmt.Sprintf("rec%d", n))
	}
	fmt.Println("Deleting these records, whose A-values are less than 25.")
	count := 0
	if err := tableScan.BeforeFirst(); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	for {
		next, err := tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !next {
			break
		}
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
			rid, err := tableScan.GetRID()
			if err != nil {
				t.Fatalf("failed to get rid: %v", err)
			}
			fmt.Printf("deleting record from slot %s: {%d, %s}\n", rid.String(), a, b)
			if err := tableScan.Delete(); err != nil {
				t.Fatalf("failed to delete: %v", err)
			}
		}
	}
	fmt.Printf("%d values under 25 were deleted\n", count)

	fmt.Println("Here are the remaining records.")
	if err := tableScan.BeforeFirst(); err != nil {
		t.Fatalf("failed to before first: %v", err)
	}
	for {
		next, err := tableScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !next {
			break
		}
		a, err := tableScan.GetInt("A")
		if err != nil {
			t.Fatalf("failed to get int: %v", err)
		}
		b, err := tableScan.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string: %v", err)
		}
		rid, err := tableScan.GetRID()
		if err != nil {
			t.Fatalf("failed to get rid: %v", err)
		}
		fmt.Printf("slot %s: {%d, %s}\n", rid.String(), a, b)
	}
	tableScan.Close()
	if err := transaction.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
