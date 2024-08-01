package record_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestRecord(t *testing.T) {
	t.Parallel()
	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "recordtest"), 400, 8)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx()
	if err != nil {
		t.Fatal(err)
	}

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
	blk, err := tx.Append("testfile")
	if err != nil {
		t.Fatalf("Failed to append block: %v", err)
	}
	if err := tx.Pin(blk); err != nil {
		t.Fatalf("Failed to pin block: %v", err)
	}
	recordPage, err := record.NewRecordPage(tx, blk, layout)
	if err != nil {
		t.Fatalf("Failed to create record page: %v", err)
	}
	if err = recordPage.Format(); err != nil {
		t.Fatalf("Failed to format record page: %v", err)
	}

	fmt.Println("Filling the page with random records.")
	slot, err := recordPage.InsertAfter(-1)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}
	for slot >= 0 {
		n := int32(rand.Intn(50))
		err = recordPage.SetInt(slot, "A", n)
		if err != nil {
			t.Fatalf("Failed to set int: %v", err)
		}
		err = recordPage.SetString(slot, "B", fmt.Sprintf("rec%d", n))
		if err != nil {
			t.Fatalf("Failed to set string: %v", err)
		}
		fmt.Printf("inserting into slot %d: {%d, %s}\n", slot, n, fmt.Sprintf("rec%d", n))
		slot, err = recordPage.InsertAfter(slot)
		if err != nil {
			t.Fatalf("Failed to get insert after record: %v", err)
		}
	}
	fmt.Println("Deleting these records, whose A-values are less than 25.")
	count := 0
	slot, err = recordPage.NextAfter(-1)
	if err != nil {
		t.Fatalf("Failed to get next slot: %v", err)
	}
	for slot >= 0 {
		a, err := recordPage.GetInt(slot, "A")
		if err != nil {
			t.Fatalf("Failed to get int: %v", err)
		}
		b, err := recordPage.GetString(slot, "B")
		if err != nil {
			t.Fatalf("Failed to get string: %v", err)
		}
		if int(a) < 25 {
			count++
			fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
			err = recordPage.Delete(slot)
			if err != nil {
				t.Fatalf("Failed to delete: %v", err)
			}
		}
		slot, err = recordPage.NextAfter(slot)
		if err != nil {
			t.Fatalf("Failed to get next slot: %v", err)
		}
	}
	fmt.Printf("%d values under 25 were deleted.\n", count)
	fmt.Println("Here are the remaining records.")
	slot, err = recordPage.NextAfter(-1)
	if err != nil {
		t.Fatalf("Failed to get next slot: %v", err)
	}
	for slot >= 0 {
		a, err := recordPage.GetInt(slot, "A")
		if err != nil {
			t.Fatalf("Failed to get int: %v", err)
		}
		b, err := recordPage.GetString(slot, "B")
		if err != nil {
			t.Fatalf("Failed to get string: %v", err)
		}
		fmt.Printf("slot %d: {%d, %s}\n", slot, a, b)
		slot, err = recordPage.NextAfter(slot)
		if err != nil {
			t.Fatalf("Failed to get next slot: %v", err)
		}
	}
	tx.Unpin(blk)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
