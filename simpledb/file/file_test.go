package file_test

import (
	"fmt"
	"testing"

	"simpledb/file"
	"simpledb/server"
)

func TestFile(t *testing.T) {
	db := server.NewSimpleDB("filetest", 400)
	fm := db.FileManager()

	blk := file.NewBlockID("testfile", 2)
	p1 := file.NewPage(fm.BlockSize())
	pos1 := 88
	p1.SetString(pos1, "abcdefghijklm")
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	fm.Write(blk, p1)

	p2 := file.NewPage(fm.BlockSize())
	fm.Read(blk, p2)
	fmt.Printf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	fmt.Printf("offset %d contains %q\n", pos1, p2.GetString(pos1))
}
