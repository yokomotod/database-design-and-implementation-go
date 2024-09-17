package server

import (
	"fmt"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
	"simpledb/metadata"
	"simpledb/plan"
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
	planner         *plan.Planner
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
	return newSimpleDBWithMetadata(dirname, true, BufferSize)
}

func NewIndexedSimpleDB(dirname string) (*SimpleDB, error) {
	return newSimpleDBWithMetadata(dirname, false, BufferSize)
}

func newSimpleDBWithMetadata(dirname string, useBasic bool, bufferSize int32) (*SimpleDB, error) {
	db, err := NewSimpleDB(dirname, BlockSize, bufferSize)
	if err != nil {
		return nil, fmt.Errorf("SimpleDB: %w", err)
	}
	tx, err := db.NewTx()
	if err != nil {
		return nil, fmt.Errorf("db.NewTx: %w", err)
	}
	isNew := db.fileManager.IsNew()
	if isNew {
		fmt.Println("creating new database")
	} else {
		fmt.Println("recovering existing database")
		if err := tx.Recover(); err != nil {
			return nil, fmt.Errorf("tx.Recover: %w", err)
		}
	}
	db.metadataManager, err = metadata.NewManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("metadata.NewManager: %w", err)
	}

	queryPlanner := plan.NewBasicQueryPlanner(db.metadataManager)
	var updatePlanner plan.UpdatePlanner
	if useBasic {
		updatePlanner = plan.NewBasicUpdatePlanner(db.metadataManager)
	} else {
		updatePlanner = plan.NewIndexUpdatePlanner(db.metadataManager)
	}
	db.planner = plan.NewPlanner(queryPlanner, updatePlanner)

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("tx.Commit: %w", err)
	}
	return db, nil
}

func (db *SimpleDB) NewTx() (*tx.Transaction, error) {
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

func (db *SimpleDB) Planner() *plan.Planner {
	return db.planner
}
