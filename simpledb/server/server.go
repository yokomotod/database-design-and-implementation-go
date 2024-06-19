package server

import (
	"fmt"

	"simpledb/file"
	"simpledb/log"
)

const logFile = "simpledb.log"

type SimpleDB struct {
	fileManager *file.Manager
	logManager  *log.Manager
}

func NewSimpleDB(dbDir string, blockSize int) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	logManager, err := log.NewManager(fileManager, logFile)
	if err != nil {
		return nil, fmt.Errorf("log.NewManager: %w", err)
	}

	return &SimpleDB{
		fileManager: fileManager,
		logManager:  logManager,
	}, nil
}

func (db *SimpleDB) FileManager() *file.Manager {
	return db.fileManager
}

func (db *SimpleDB) LogManager() *log.Manager {
	return db.logManager
}
