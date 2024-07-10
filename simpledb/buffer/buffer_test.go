package buffer_test

import (
	"fmt"
	"path"
	"testing"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/server"
)

func TestBuffer(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	for i := range 5 {
		db, err := server.NewSimpleDB(path.Join(tempDir, "filetest"), 400, 3) // only 3 buffers
		if err != nil {
			t.Fatalf("NewSimpleDB: %v", err)
		}

		bm := db.BufferManager()

		buff1, err := bm.Pin(file.NewBlockID("testfile", 1))
		if err != nil {
			t.Fatalf("bm.Pin(1): %v", err)
		}

		p := buff1.Contents()
		n := p.GetInt(80)

		if int(n) != i {
			t.Fatalf("expected %d, got %d", i, n)
		}

		p.SetInt(80, n+1)
		buff1.SetModified(1, 0) // placeholder values
		fmt.Printf("The new value is %d\n", (n + 1))
		bm.Unpin(buff1)

		// One of these pins will flush buff1 to disk:
		buff2, err := bm.Pin(file.NewBlockID("testfile", 2))
		if err != nil {
			t.Fatalf("bm.Pin(2): %v", err)
		}
		/* buff3 */ _, err = bm.Pin(file.NewBlockID("testfile", 3))
		if err != nil {
			t.Fatalf("bm.Pin(3): %v", err)
		}
		/* buff4 */ _, err = bm.Pin(file.NewBlockID("testfile", 4))
		if err != nil {
			t.Fatalf("bm.Pin(4): %v", err)
		}

		bm.Unpin(buff2)

		buff1, err = bm.Pin(file.NewBlockID("testfile", 1))
		if err != nil {
			t.Fatalf("bm.Pin(1): %v", err)
		}
		p = buff1.Contents()
		p.SetInt(80, 9999)      // This modification
		buff1.SetModified(1, 0) // won't get written to disk.
	}
}

func TestBufferManager(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "buffermgrtest"), 400, 3) // only 3 buffers
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	bm := db.BufferManager()

	buff := [6]*buffer.Buffer{}
	buff[0], err = bm.Pin(file.NewBlockID("testfile", 0))
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}
	buff[1], err = bm.Pin(file.NewBlockID("testfile", 1))
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}
	buff[2], err = bm.Pin(file.NewBlockID("testfile", 2))
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}

	bm.Unpin(buff[1])
	buff[1] = nil

	buff[3], err = bm.Pin(file.NewBlockID("testfile", 0)) // block 0 pinned twice
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}

	buff[4], err = bm.Pin(file.NewBlockID("testfile", 1)) // block 1 repinned
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}

	fmt.Printf("Available buffers: %d\n", bm.NumAvailable())

	fmt.Println("Attempting to pin block 3...")
	buff[5], err = bm.Pin(file.NewBlockID("testfile", 3)) // will not work; no buffers left
	if err != nil {
		if err != buffer.ErrBufferAbort {
			t.Fatalf("bm.Pin: %v", err)
		}
		fmt.Println("Exception: No available buffers")
	} else {
		t.Fatalf("no error")
	}

	bm.Unpin(buff[2])
	buff[2] = nil

	buff[5], err = bm.Pin(file.NewBlockID("testfile", 3)) // now this works
	if err != nil {
		t.Fatalf("bm.Pin: %v", err)
	}

	wants := []int32{0, -1, -1, 0, 1, 3}
	fmt.Println("Final Buffer Allocation:")
	for i, b := range buff {
		if b != nil {
			if wants[i] < 0 {
				t.Errorf("buff[%d] pinned to block %v, but should be unpinned", i, b.Block().Number)
			}
			if b.Block().Number != wants[i] {
				t.Errorf("buff[%d] pinned to block %v, but should be block %d", i, b.Block().Number, wants[i])
			}
		} else {
			if wants[i] >= 0 {
				t.Errorf("buff[%d] is unpinned, but should be pinned to block %d", i, wants[i])
			}

			continue
		}

		fmt.Printf("buff[%d] pinned to block %v\n", i, b.Block())
	}
}
