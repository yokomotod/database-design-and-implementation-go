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

func TestConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "concurrencytest"), 400, 8)
	if err != nil {
		t.Fatal(err)
	}

	fm := db.FileManager()
	lm := db.LogManager()
	bm := db.BufferManager()

	wg := &sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()

		txA, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err = txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txA.Pin(blk2)
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
		time.Sleep(1 * time.Second)

		t.Log("Tx A: request slock 2")
		_, err = txA.GetInt(blk2, 0)
		if err != nil {
			t.Logf("Tx A: %v, rollback", err)
			if err := txA.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx A: receive slock 2")
		if err := txA.Commit(); err != nil {
			panic(err)
		}
		t.Log("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		txB, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err = txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txB.Pin(blk2)
		if err != nil {
			panic(err)
		}
		t.Log("Tx B: request xlock 2")
		err = txB.SetInt(blk2, 0, 0, false)
		if err != nil {
			t.Logf("Tx B: %v, rollback", err)
			if err := txB.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx B: receive xlock 2")
		time.Sleep(1 * time.Second)

		t.Log("Tx B: request slock 1")
		_, err = txB.GetInt(blk1, 0)
		if err != nil {
			t.Logf("Tx B: %v, rollback", err)
			if err := txB.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx B: receive slock 1")
		if err := txB.Commit(); err != nil {
			panic(err)
		}
		t.Log("Tx B: commit")
	}()

	go func() {
		defer wg.Done()

		txC, err := tx.New(fm, lm, bm)
		if err != nil {
			panic(err)
		}
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err = txC.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txC.Pin(blk2)
		if err != nil {
			panic(err)
		}

		time.Sleep(500 * time.Millisecond)
		t.Log("Tx C: request xlock 1")
		err = txC.SetInt(blk1, 0, 0, false)
		if err != nil {
			t.Logf("Tx C: %v, rollback", err)
			if err := txC.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)
		t.Log("Tx C: request slock 2")
		_, err = txC.GetInt(blk2, 0)
		if err != nil {
			t.Logf("Tx B: %v, rollback", err)
			if err := txC.Rollback(); err != nil {
				panic(err)
			}
			return
		}
		t.Log("Tx C: receive slock 2")
		if err := txC.Commit(); err != nil {
			panic(err)
		}
		t.Log("Tx C: commit")
	}()

	wg.Wait()
}
