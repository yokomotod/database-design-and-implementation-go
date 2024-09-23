package buffer

import (
	"errors"
	"fmt"
	"simpledb/file"
	"simpledb/util/logger"
	"sync"
)

type Buffer struct {
	logger    *logger.Logger
	debugName string

	fileManager *file.Manager
	contents    *file.Page
	block       file.BlockID
	pins        int32
	txNum       int32
	lsn         int32
}

func NewBuffer(fm *file.Manager, debugName string) *Buffer {
	return &Buffer{
		logger:    logger.New("buffer.Buffer", logger.Trace),
		debugName: debugName,

		fileManager: fm,
		txNum:       -1,
		contents:    file.NewPage(fm.BlockSize()),
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() file.BlockID {
	return b.block
}

func (b *Buffer) SetModified(txNum int32, lsn int32) {
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) Pin() {
	b.pins++
	b.logger.Tracef("(%q) Pin(): buffer[%s]=%dpins block %+v", b.block.FileName, b.debugName, b.pins, b.block)
}

func (b *Buffer) Unpin() {
	if b.pins <= 0 {
		panic(fmt.Sprintf("(%q) unpin() called on unpinned buffer[%s]=%dpins block %+v", b.block.FileName, b.debugName, b.pins, b.block))
	}

	b.pins--
	b.logger.Tracef("(%q) Unpin(): buffer[%s]=%dpins block %+v", b.block.FileName, b.debugName, b.pins, b.block)
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) AssignToBlock(blk file.BlockID) (bool, error) {
	b.logger.Tracef("(%q) AssignToBlock(): buffer[%s] old=%+v new=%+v", blk.FileName, b.debugName, b.block, blk)
	flushed, err := b.flush()
	if err != nil {
		return false, fmt.Errorf("b.flush: %w", err)
	}
	b.block = blk

	b.logger.Tracef("(%q) AssignToBlock(): read block %+v to buffer[%s]", blk.FileName, blk, b.debugName)
	if err := b.fileManager.Read(blk, b.contents); err != nil {
		return false, fmt.Errorf("fileManager.Read: %w", err)
	}
	b.pins = 0

	return flushed, nil
}

func (b *Buffer) flush() (bool, error) {
	if b.txNum <= 0 {
		return false, nil
	}

	b.logger.Tracef("(%q) flush(): write buffer[%s] to block %+v", b.block.FileName, b.debugName, b.block)
	if err := b.fileManager.Write(b.block, b.contents); err != nil {
		return false, fmt.Errorf("fileManager.Write: %w", err)
	}
	b.txNum = -1

	return true, nil
}

type Manager struct {
	logger *logger.Logger

	bufferPool   []*Buffer
	numAvailable int32
	mux          *sync.Mutex
}

func NewManager(fm *file.Manager, buffSize int32) *Manager {
	logger := logger.New("buffer.Manager", logger.Trace)

	logger.Tracef("NewManager(): bufferPool=%d", buffSize)
	bufferPool := make([]*Buffer, buffSize)
	for i := range bufferPool {
		bufferPool[i] = NewBuffer(fm, fmt.Sprintf("#%d/%d", i, buffSize))
	}

	return &Manager{
		logger: logger,

		bufferPool:   bufferPool,
		numAvailable: buffSize,
		mux:          &sync.Mutex{},
	}
}

func (bm *Manager) FlushAll(txNum int32) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	for _, buf := range bm.bufferPool {
		if buf.txNum == txNum {
			buf.flush()
		}
	}
}

func (bm *Manager) NumAvailable() int32 {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	return bm.numAvailable
}

func (bm *Manager) Unpin(buff *Buffer) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		bm.logger.Tracef("(%q) Unpin(): numAvailable=%d/%d", buff.Block().FileName, bm.numAvailable, len(bm.bufferPool))
		// TODO: notifyAll();
	}
}

var ErrBufferAbort = errors.New("buffer abort")

func (bm *Manager) Pin(blk file.BlockID) (*Buffer, int, error) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	buff, blocksAccessed, err := bm.tryToPin(blk)
	if err != nil {
		return nil, 0, fmt.Errorf("bm.tryToPin: %w", err)
	}
	if buff == nil {
		return nil, 0, ErrBufferAbort
	}

	return buff, blocksAccessed, nil
}

func (bm *Manager) tryToPin(blk file.BlockID) (*Buffer, int, error) {
	buff := bm.findExistingBuffer(blk)
	blocksAccessed := 0

	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, 0, nil
		}
		flushed, err := buff.AssignToBlock(blk)
		if err != nil {
			return nil, 0, fmt.Errorf("buff.AssignToBlock: %w", err)
		}
		if flushed {
			blocksAccessed = 2
		} else {
			blocksAccessed = 1
		}
	}
	if !buff.IsPinned() {
		bm.numAvailable--
		bm.logger.Tracef("(%q) tryToPin(): numAvailable=%d/%d", buff.Block().FileName, bm.numAvailable, len(bm.bufferPool))
	}
	buff.Pin()
	return buff, blocksAccessed, nil
}

func (bm *Manager) findExistingBuffer(blk file.BlockID) *Buffer {
	for _, buff := range bm.bufferPool {
		b := buff.Block()
		if b == blk {
			return buff
		}
	}

	return nil
}

func (bm *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferPool {
		if !buff.IsPinned() {
			return buff
		}
	}

	return nil
}
