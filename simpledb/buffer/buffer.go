package buffer

import (
	"errors"

	"simpledb/file"
)

type Buffer struct {
	fileManager *file.Manager
	contents    *file.Page
	block       file.BlockID
	pins        int32
	txNum       int32
	lsn         int32
}

func NewBuffer(fm *file.Manager) *Buffer {
	return &Buffer{
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
}

func (b *Buffer) Unpin() {
	b.pins--
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) AssignToBlock(blk file.BlockID) {
	b.flush()
	b.block = blk
	b.fileManager.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txNum <= 0 {
		return
	}

	b.fileManager.Write(b.block, b.contents)
	b.txNum = -1
}

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int32
}

func NewManager(fm *file.Manager, buffSize int32) *Manager {
	bufferPool := make([]*Buffer, buffSize)
	for i := range bufferPool {
		bufferPool[i] = NewBuffer(fm)
	}

	return &Manager{
		bufferPool:   bufferPool,
		numAvailable: buffSize,
	}
}

func (bm *Manager) FlushAll(txNum int32) {
	for _, buf := range bm.bufferPool {
		if buf.txNum == txNum {
			buf.flush()
		}
	}
}

func (bm *Manager) NumAvailable() int32 {
	return bm.numAvailable
}

func (bm *Manager) Unpin(buff *Buffer) {
	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		// TODO: notifyAll();
	}
}

var ErrBufferAbort = errors.New("buffer abort")

func (bm *Manager) Pin(blk file.BlockID) (*Buffer, error) {
	buff := bm.tryToPin(blk)
	if buff == nil {
		return nil, ErrBufferAbort
	}

	return buff, nil
}

func (bm *Manager) tryToPin(blk file.BlockID) *Buffer {
	buff := bm.findExistingBuffer(blk)

	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.AssignToBlock(blk)
	}
	if !buff.IsPinned() {
		bm.numAvailable--
	}
	buff.Pin()
	return buff
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
