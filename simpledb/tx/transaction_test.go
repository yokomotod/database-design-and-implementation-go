package tx_test

import (
	"fmt"
	"path"
	"testing"

	"simpledb/file"
	"simpledb/server"
	"simpledb/tx"
)

func TestTransaction(t *testing.T) {
	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "txtest"), 400, 8)
	if err != nil {
		t.Fatal(err)
	}

	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	tx1 := tx.New(fm, lm, bm)
	blk := file.NewBlockID("testfile", 1)
	err = tx1.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	// The block initially contains unknown bytes,
	// so don't log those values here.
	err = tx1.SetInt(blk, 80, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	err = tx1.SetString(blk, 40, "one", false)
	if err != nil {
		t.Fatal(err)
	}
	tx1.Commit()

	tx2 := tx.New(fm, lm, bm)
	err = tx2.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	ival := tx2.GetInt(blk, 80)
	if ival != 1 {
		t.Fatalf("expected 1, got %d", ival)
	}
	sval := tx2.GetString(blk, 40)
	if sval != "one" {
		t.Fatalf("expected one, got %s", sval)
	}
	fmt.Printf("initial value at location 80 = %d\n", ival)
	fmt.Printf("initial value at location 40 = %s\n", sval)
	newival := ival + 1
	newsval := sval + "!"
	tx2.SetInt(blk, 80, newival, true)
	tx2.SetString(blk, 40, newsval, true)
	tx2.Commit()

	tx3 := tx.New(fm, lm, bm)
	err = tx3.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	ival = tx3.GetInt(blk, 80)
	if ival != 2 {
		t.Fatalf("expected 2, got %d", ival)
	}
	sval = tx3.GetString(blk, 40)
	if sval != "one!" {
		t.Fatalf("expected one!, got %s", sval)
	}
	fmt.Printf("initial value at location 80 = %d\n", ival)
	fmt.Printf("initial value at location 40 = %s\n", sval)
	tx3.SetInt(blk, 80, 9999, true)
	ival = tx3.GetInt(blk, 80)
	if ival != 9999 {
		t.Fatalf("expected 9999, got %d", ival)
	}
	fmt.Printf("pre-rollback value at location 80 = %d\n", ival)
	tx3.Rollback()

	tx4 := tx.New(fm, lm, bm)
	err = tx4.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	ival = tx4.GetInt(blk, 80)
	if ival != 2 {
		t.Fatalf("expected 2, got %d", ival)
	}
	fmt.Printf("pre-recover value at location 80 = %d\n", ival)
	tx4.Commit()
}
