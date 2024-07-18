package file_test

import (
	"path"
	"testing"

	"simpledb/file"
	"simpledb/server"
)

func TestFile(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB( /*dbDir:*/ path.Join(t.TempDir(), "filetest") /*blockSize*/, 400, 8)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	fm := db.FileManager()

	p1 := file.NewPage(fm.BlockSize())
	var pos1 int32 = 88
	strVal := "abcdefghijklm"
	p1.SetString(pos1, strVal)

	size := file.MaxLength(int32(len(strVal)))
	pos2 := pos1 + size
	intVar := int32(345)
	p1.SetInt(pos2, intVar)

	blk := file.NewBlockID("testfile", 2)
	err = fm.Write(blk, p1)
	if err != nil {
		t.Fatalf("fm.Write: %v", err)
	}

	p2 := file.NewPage(fm.BlockSize())
	err = fm.Read(blk, p2)
	if err != nil {
		t.Fatalf("fm.Read: %v", err)
	}

	if p2.GetInt(pos2) != intVar {
		t.Errorf("expected %d, got %d", intVar, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != strVal {
		t.Errorf("expected %q, got %q", strVal, p2.GetString(pos1))
	}
}
