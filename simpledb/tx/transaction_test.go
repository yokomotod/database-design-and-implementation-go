package tx_test

import (
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

	tx1, err := tx.New(fm, lm, bm)
	if err != nil {
		t.Fatal(err)
	}
	blk := file.NewBlockID("testfile", 1)
	if err := tx1.Pin(blk); err != nil {
		t.Fatal(err)
	}
	// The block initially contains unknown bytes,
	// so don't log those values here.
	if err := tx1.SetInt(blk, 80, 1, false); err != nil {
		t.Fatal(err)
	}
	if err := tx1.SetString(blk, 40, "one", false); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, err := tx.New(fm, lm, bm)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Pin(blk); err != nil {
		t.Fatal(err)
	}
	ival, err := tx2.GetInt(blk, 80)
	if err != nil {
		t.Fatal(err)
	}
	if ival != 1 {
		t.Fatalf("expected 1, got %d", ival)
	}
	sval, err := tx2.GetString(blk, 40)
	if err != nil {
		t.Fatal(err)
	}
	if sval != "one" {
		t.Fatalf("expected one, got %s", sval)
	}
	t.Logf("initial value at location 80 = %d\n", ival)
	t.Logf("initial value at location 40 = %s\n", sval)
	newival := ival + 1
	newsval := sval + "!"
	if err := tx2.SetInt(blk, 80, newival, true); err != nil {
		t.Fatal(err)
	}
	if err := tx2.SetString(blk, 40, newsval, true); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Logf("start transaction 3\n")
	tx3, err := tx.New(fm, lm, bm)
	if err != nil {
		t.Fatal(err)
	}
	err = tx3.Pin(blk)
	if err != nil {
		t.Fatal(err)
	}
	ival, err = tx3.GetInt(blk, 80)
	if err != nil {
		t.Fatal(err)
	}
	if ival != 2 {
		t.Fatalf("expected 2, got %d", ival)
	}
	sval, err = tx3.GetString(blk, 40)
	if err != nil {
		t.Fatal(err)
	}
	if sval != "one!" {
		t.Fatalf("expected one!, got %s", sval)
	}
	t.Logf("initial value at location 80 = %d\n", ival)
	t.Logf("initial value at location 40 = %s\n", sval)
	if err := tx3.SetInt(blk, 80, 9999, true); err != nil {
		t.Fatal(err)
	}
	ival, err = tx3.GetInt(blk, 80)
	if err != nil {
		t.Fatal(err)
	}
	if ival != 9999 {
		t.Fatalf("expected 9999, got %d", ival)
	}
	t.Logf("pre-rollback value at location 80 = %d\n", ival)
	if err := tx3.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx4, err := tx.New(fm, lm, bm)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx4.Pin(blk); err != nil {
		t.Fatal(err)
	}
	ival, err = tx4.GetInt(blk, 80)
	if err != nil {
		t.Fatal(err)
	}
	if ival != 2 {
		t.Fatalf("expected 2, got %d", ival)
	}
	t.Logf("pre-recover value at location 80 = %d\n", ival)
	if err := tx4.Commit(); err != nil {
		t.Fatal(err)
	}
}
