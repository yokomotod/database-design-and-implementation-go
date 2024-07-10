package tx_test

import (
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"simpledb/file"
	"simpledb/server"
	"simpledb/tx"
)

func TestConcurrency(t *testing.T) {
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

		txA := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err := txA.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txA.Pin(blk2)
		if err != nil {
			panic(err)
		}
		fmt.Println("Tx A: request slock 1")
		txA.GetInt(blk1, 0)
		fmt.Println("Tx A: receive slock 1")
		time.Sleep(1 * time.Second)

		fmt.Println("Tx A: request slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive slock 2")
		txA.Commit()
		fmt.Println("Tx A: commit")
	}()

	go func() {
		defer wg.Done()

		txB := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err := txB.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txB.Pin(blk2)
		if err != nil {
			panic(err)
		}
		fmt.Println("Tx B: request xlock 2")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive xlock 2")
		time.Sleep(1 * time.Second)

		fmt.Println("Tx B: request slock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B: receive slock 1")
		txB.Commit()
		fmt.Println("Tx B: commit")
	}()

	go func() {
		defer wg.Done()

		txC := tx.New(fm, lm, bm)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		err := txC.Pin(blk1)
		if err != nil {
			panic(err)
		}
		err = txC.Pin(blk2)
		if err != nil {
			panic(err)
		}

		time.Sleep(500 * time.Millisecond)
		fmt.Println("Tx C: request xlock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx C: request slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive slock 2")
		txC.Commit()
		fmt.Println("Tx C: commit")
	}()

	wg.Wait()
	t.Fatalf("fail")
}
