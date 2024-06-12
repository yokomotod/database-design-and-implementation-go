package server

import (
	"fmt"

	"simpledb/file"
)

type SimpleDB struct {
	fileManager *file.Manager
}

func NewSimpleDB(dbDir string, blockSize int) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	return &SimpleDB{
		fileManager: fileManager,
	}, nil
}

func (db *SimpleDB) FileManager() *file.Manager {
	return db.fileManager
}
