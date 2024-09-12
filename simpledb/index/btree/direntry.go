package btree

import "simpledb/query"

type DirEntry struct {
	dataval *query.Constant
	block   int32
}

func NewDirEntry(dataval *query.Constant, block int32) *DirEntry {
	return &DirEntry{dataval: dataval, block: block}
}
