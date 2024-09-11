package buffer

import (
	"errors"
	"fmt"
	"simpledb/file"
	"simpledb/util/logger"
	"sync"
)

type Buffer struct {
	logger *logger.Logger

	fileManager *file.Manager
	contents    *file.Page
	block       file.BlockID
	pins        int32
	txNum       int32
	lsn         int32

	debugName string
}

func NewBuffer(fm *file.Manager, debugName string) *Buffer {
	return &Buffer{
		logger: logger.New("buffer.Buffer", logger.Trace),

		fileManager: fm,
		txNum:       -1,
		contents:    file.NewPage(fm.BlockSize()),
		debugName:   debugName,
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
	b.logger.Tracef("Pin(): buffer[%s]=%dpins block %+v", b.debugName, b.pins, b.block)
}

func (b *Buffer) Unpin() {
	if b.pins <= 0 {
		panic(fmt.Sprintf("unpin() called on unpinned buffer[%s]=%dpins block %+v", b.debugName, b.pins, b.block))
	}

	b.pins--
	b.logger.Tracef("Unpin(): buffer[%s]=%dpins block %+v", b.debugName, b.pins, b.block)
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) AssignToBlock(blk file.BlockID) error {
	b.flush()
	b.block = blk

	b.logger.Tracef("AssignToBlock(): read block %+v to buffer[%s]", blk, b.debugName)
	if err := b.fileManager.Read(blk, b.contents); err != nil {
		return fmt.Errorf("fileManager.Read: %w", err)
	}
	b.pins = 0

	return nil
}

func (b *Buffer) flush() error {
	if b.txNum <= 0 {
		return nil
	}

	b.logger.Tracef("flush(): write buffer[%s] to block %+v", b.debugName, b.block)
	if err := b.fileManager.Write(b.block, b.contents); err != nil {
		return fmt.Errorf("fileManager.Write: %w", err)
	}
	b.txNum = -1

	return nil
}

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int32
	mux          *sync.Mutex
}

func NewManager(fm *file.Manager, buffSize int32) *Manager {
	bufferPool := make([]*Buffer, buffSize)
	for i := range bufferPool {
		bufferPool[i] = NewBuffer(fm, fmt.Sprintf("%d/%d", i, buffSize))
	}

	return &Manager{
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
		// TODO: notifyAll();
	}
}

var ErrBufferAbort = errors.New("buffer abort")

func (bm *Manager) Pin(blk file.BlockID) (*Buffer, error) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	buff, err := bm.tryToPin(blk)
	if err != nil {
		return nil, fmt.Errorf("bm.tryToPin: %w", err)
	}
	if buff == nil {
		return nil, ErrBufferAbort
	}

	return buff, nil
}

func (bm *Manager) tryToPin(blk file.BlockID) (*Buffer, error) {
	buff := bm.findExistingBuffer(blk)

	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil
		}
		if err := buff.AssignToBlock(blk); err != nil {
			return nil, fmt.Errorf("buff.AssignToBlock: %w", err)
		}
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff, nil
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
