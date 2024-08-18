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

		txA, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		err = txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx A: request slock 1")
		_, err = txA.GetInt(blk1, 0)
		if err != nil {
			t.Logf("Tx A: %v, rollback", err)
			if err := txA.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx A: receive slock 1")
		time.Sleep(12 * time.Second)

		if err := txA.Commit(); err != nil {
			panic(err)
		}
		t.Log("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		// txA に先にロックをとってもらう
		time.Sleep(1 * time.Second)

		txB, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		err = txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx B: request xlock 1")
		err = txB.SetInt(blk1, 0, 0, false)
		if err != nil {
			// Timeout でこちらの分岐に入ることが期待値
			t.Logf("Tx B: %v, rollback", err)
			if err := txB.Rollback(); err != nil {
				panic(err)
			}
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

		txA, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		err = txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx A: request xlock 1")
		err = txA.SetInt(blk1, 0, 0, false)
		if err != nil {
			t.Logf("Tx A: %v, rollback", err)
			if err := txA.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx A: receive xlock 1")
		time.Sleep(12 * time.Second)

		if err := txA.Commit(); err != nil {
			panic(err)
		}
		t.Log("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		// txA に先にロックをとってもらう
		time.Sleep(1 * time.Second)

		txB, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		err = txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		t.Log("Tx B: request slock 1")
		_, err = txB.GetInt(blk1, 0)
		if err != nil {
			// Timeout でこちらの分岐に入ることが期待値
			t.Logf("Tx B: %v, rollback", err)
			if err := txB.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Errorf("Tx B: Does not reach here")
	}()

	wg.Wait()
}

func TestConcurrencyXLockMany(t *testing.T) {
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

	n := 10

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for i := range n {
		go func(i int) {
			defer wg.Done()

			txA, err := tx.New(fm, lm, bm)
			if err != nil {
				panic(err)
			}
			blk1 := file.NewBlockID("testfile", 1)
			err = txA.Pin(blk1)
			if err != nil {
				panic(err)
			}
			t.Logf("Tx %d: request xlock 1", i)
			err = txA.SetInt(blk1, 0, 0, false)
			if err != nil {
				panic(err)
			}
			t.Logf("Tx %d: receive xlock 1", i)
			time.Sleep(50 * time.Millisecond)

			t.Logf("Tx %d: commit", i)
			if err := txA.Commit(); err != nil {
				panic(err)
			}
		}(i)

		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}
