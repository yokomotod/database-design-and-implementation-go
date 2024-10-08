package log

import (
	"fmt"
	"sync"

	"simpledb/file"
	"simpledb/util/logger"
)

type LogIterator struct {
	fileManager *file.Manager
	blk         file.BlockID
	page        *file.Page
	currentPos  int32
	boundary    int32
}

func NewIterator(fileManager *file.Manager, blk file.BlockID) (*LogIterator, error) {
	b := make([]byte, fileManager.BlockSize())
	page := file.NewPageWith(b)

	it := &LogIterator{
		fileManager: fileManager,
		blk:         blk,
		page:        page,
		currentPos:  0,
		boundary:    0,
	}

	if err := it.moveToBlock(blk); err != nil {
		return nil, fmt.Errorf("it.moveToBlock: %w", err)
	}

	return it, nil
}

func (it *LogIterator) moveToBlock(blk file.BlockID) error {
	if err := it.fileManager.Read(blk, it.page); err != nil {
		return fmt.Errorf("fileManager.Read: %w", err)
	}
	it.boundary = it.page.GetInt(0)
	it.currentPos = it.boundary

	return nil
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fileManager.BlockSize() || it.blk.Number > 0
}

func (it *LogIterator) Next() ([]byte, error) {
	if it.currentPos == it.fileManager.BlockSize() {
		it.blk = file.NewBlockID(it.blk.FileName, it.blk.Number-1)
		if err := it.moveToBlock(it.blk); err != nil {
			return nil, fmt.Errorf("it.moveToBlock: %w", err)
		}
	}

	rec := it.page.GetBytes(it.currentPos)
	it.currentPos += file.Int32Bytes + int32(len(rec))

	return rec, nil
}

type Manager struct {
	logger *logger.Logger

	fileManager *file.Manager
	logFile     string
	logPage     *file.Page
	currentBlk  file.BlockID
	// LSN: log sequence number
	latestLSN    int32
	lastSavedLSN int32
	mux          *sync.Mutex
}

func NewManager(fileManager *file.Manager, logFile string) (*Manager, error) {
	logger := logger.New("log.Manager", logger.Debug)
	logger.Tracef("(%q) NewManager", logFile)

	b := make([]byte, fileManager.BlockSize())
	logPage := file.NewPageWith(b)

	logSize, err := fileManager.Length(logFile)
	if err != nil {
		return nil, fmt.Errorf("fileManager.Length: %w", err)
	}

	lm := &Manager{
		logger: logger,

		fileManager: fileManager,
		logFile:     logFile,
		logPage:     logPage,
		mux:         &sync.Mutex{},
	}

	if logSize == 0 {
		logger.Tracef("(%q) NewManager: logSize == 0, appendNewBlock", logFile)
		lm.currentBlk, err = lm.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("lm.appendNewBlock: %w", err)
		}
	} else {
		lm.currentBlk = file.NewBlockID(logFile, logSize-1)
		if err := fileManager.Read(lm.currentBlk, logPage); err != nil {
			return nil, fmt.Errorf("fileManager.Read: %w", err)
		}
	}

	return lm, nil
}

func (lm *Manager) appendNewBlock() (file.BlockID, error) {
	blk, err := lm.fileManager.Append(lm.logFile)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("fileManager.Append: %w", err)
	}

	lm.logPage.SetInt(0, int32(lm.fileManager.BlockSize()))
	if err := lm.fileManager.Write(blk, lm.logPage); err != nil {
		return file.BlockID{}, fmt.Errorf("fileManager.Write: %w", err)
	}

	return blk, nil
}

func (lm *Manager) Flush(lsn int32) {
	if lsn < lm.lastSavedLSN {
		return
	}

	lm.logger.Tracef("(%q) Flush(): lsn(%d) <= lastSavedLSN(%d)", lm.logFile, lsn, lm.lastSavedLSN)
	lm.flush()
}

func (lm *Manager) flush() error {
	if err := lm.fileManager.Write(lm.currentBlk, lm.logPage); err != nil {
		return fmt.Errorf("fileManager.Write: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN

	return nil
}

func (lm *Manager) Iterator() (*LogIterator, error) {
	lm.flush()
	return NewIterator(lm.fileManager, lm.currentBlk)
}

func (lm *Manager) Append(logRecord []byte) (int32, error) {
	lm.mux.Lock()
	defer lm.mux.Unlock()

	boundary := lm.logPage.GetInt(0)
	recordSize := int32(len(logRecord))
	bytesNeeded := recordSize + file.Int32Bytes

	lm.logger.Tracef("(%q) Append(): check boundary(%d) - bytesNeeded(%d) < Int32Bytes(%d)", lm.logFile, boundary, bytesNeeded, file.Int32Bytes)
	if boundary-bytesNeeded < file.Int32Bytes { // It doesn't fit
		lm.logger.Tracef("(%q) Append(): flush()", lm.logFile)
		lm.flush() // so move to the next block.
		lm.logger.Tracef("(%q) Append(): appendNewBlock()", lm.logFile)
		currentBlk, err := lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("lm.appendNewBlock: %w", err)
		}
		lm.currentBlk = currentBlk
		boundary = lm.logPage.GetInt(0)
	}
	recPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recPos, logRecord)
	lm.logPage.SetInt(0, int32(recPos)) // the new boundary

	lm.latestLSN += 1

	return lm.latestLSN, nil
}
