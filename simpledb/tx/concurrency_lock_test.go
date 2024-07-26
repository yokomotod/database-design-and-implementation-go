package tx_test

import (
	"path"
	"sync"
	"testing"
	"time"

	"simpledb/file"
	"simpledb/server"
	"simpledb/tx"
)

func TestConcurrencySLockTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode, this takes long time for checking timeout.")
	}

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "concurrencytest"), 400, 8)
	if err != nil {
		t.Fatal(err)
	}

	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		txA := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		err := txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx A: request slock 1")
		_, err = txA.GetInt(blk1, 0)
		if err != nil {
			t.Logf("Tx A: %v, rollback", err)
			txA.Rollback()
			return
		}
		t.Log("Tx A: receive slock 1")
		time.Sleep(12 * time.Second)

		txA.Commit()
		t.Log("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		// txA に先にロックをとってもらう
		time.Sleep(1 * time.Second)

		txB := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		err := txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx B: request xlock 1")
		err = txB.SetInt(blk1, 0, 0, false)
		if err != nil {
			// Timeout でこちらの分岐に入ることが期待値
			t.Logf("Tx B: %v, rollback", err)
			txB.Rollback()
			return
		}
		t.Errorf("Tx B: Does not reach here")
	}()

	wg.Wait()
}

func TestConcurrencyXLockTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode, this takes long time for checking timeout.")
	}

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "concurrencytest"), 400, 8)
	if err != nil {
		t.Fatal(err)
	}

	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		txA := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		err := txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx A: request xlock 1")
		err = txA.SetInt(blk1, 0, 0, false)
		if err != nil {
			t.Logf("Tx A: %v, rollback", err)
			txA.Rollback()
			return
		}
		t.Log("Tx A: receive xlock 1")
		time.Sleep(12 * time.Second)

		txA.Commit()
		t.Log("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		// txA に先にロックをとってもらう
		time.Sleep(1 * time.Second)

		txB := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		err := txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx B: request slock 1")
		_, err = txB.GetInt(blk1, 0)
		if err != nil {
			// Timeout でこちらの分岐に入ることが期待値
			t.Logf("Tx B: %v, rollback", err)
			txB.Rollback()
			return
		}
		t.Errorf("Tx B: Does not reach here")
	}()

	wg.Wait()
}
