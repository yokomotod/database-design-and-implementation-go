package server

import (
	"fmt"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
	"simpledb/metadata"
	"simpledb/tx"
)

const BlockSize = 400
const BufferSize = 8
const logFile = "simpledb.log"

type SimpleDB struct {
	fileManager     *file.Manager
	logManager      *log.Manager
	bufferManager   *buffer.Manager
	metadataManager *metadata.Manager
}

func NewSimpleDB(dbDir string, blockSize, bufferSize int32) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, blockSize)
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

func NewSimpleDBWithMetadata(dirname string) (*SimpleDB, error) {
	db, err := NewSimpleDB(dirname, BlockSize, BufferSize)
	if err != nil {
		return nil, fmt.Errorf("SimpleDB: %w", err)
	}
	tx := db.NewTx()
	isNew := db.fileManager.IsNew()
	if isNew {
		fmt.Println("creating new database")
	} else {
		fmt.Println("recovering existing database")
		tx.Recover()
	}
	db.metadataManager, err = metadata.NewManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata.NewManager: %w", err)
	}
	tx.Commit()
	return db, nil
}

func (db *SimpleDB) NewTx() *tx.Transaction {
	return tx.New(
		db.fileManager,
		db.logManager,
		db.bufferManager,
	)
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

func (db *SimpleDB) MetadataManager() *metadata.Manager {
	return db.metadataManager
}
