package server

import "simpledb/file"

type SimpleDB struct {
	fileManager *file.Manager
}

func NewSimpleDB(dbDir string, blockSize int) *SimpleDB {
	return &SimpleDB{
		fileManager: file.NewManager(dbDir, blockSize),
	}
}

func (db *SimpleDB) FileManager() *file.Manager {
	return db.fileManager
}
