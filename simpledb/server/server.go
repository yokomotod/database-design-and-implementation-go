package server

import (
	"fmt"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
)

const logFile = "simpledb.log"

type SimpleDB struct {
	fileManager   *file.Manager
	logManager    *log.Manager
	bufferManager *buffer.Manager
}

func NewSimpleDB(dbDir string, blockSize, bufferSize int) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	logManager, err := log.NewManager(fileManager, logFile)
	if err != nil {
		return nil, fmt.Errorf("log.NewManager: %w", err)
	}

	bufferManager := buffer.NewManager(fileManager, bufferSize)
	return &SimpleDB{
		fileManager:   fileManager,
		logManager:    logManager,
		bufferManager: bufferManager,
	}, nil
}

func (db *SimpleDB) FileManager() *file.Manager {
	return db.fileManager
}

func (db *SimpleDB) LogManager() *log.Manager {
	return db.logManager
}

func (db *SimpleDB) BufferManager() *buffer.Manager {
	return db.bufferManager
}
