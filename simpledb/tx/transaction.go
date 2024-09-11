package tx

import (
	"fmt"
	"slices"
	"sync"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
	"simpledb/tx/concurrency"
	"simpledb/tx/recovery"
)

const endOfFile = -1

var (
	txMutex         = &sync.Mutex{}
	nextTxNum int32 = 0
)

type Transaction struct {
	recoveryMgr *recovery.Manager
	concurMgr   *concurrency.Manager
	bm          *buffer.Manager
	fm          *file.Manager
	txnum       int32
	mybuffers   *BufferList
}

func New(fileMgr *file.Manager, logMgr *log.Manager, bufferManager *buffer.Manager) (*Transaction, error) {
	tx := &Transaction{
		concurMgr: concurrency.New(),
		fm:        fileMgr,
		bm:        bufferManager,
		txnum:     nextTxNumber(),
		mybuffers: newBufferList(bufferManager),
	}

	var err error
	tx.recoveryMgr, err = recovery.New(tx, tx.txnum, logMgr, bufferManager)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (tx *Transaction) Commit() error {
	if err := tx.recoveryMgr.Commit(); err != nil {
		return err
	}
	tx.concurMgr.Release()
	tx.mybuffers.unpinAll()
	fmt.Printf("transaction %d committed\n", tx.txnum)

	return nil
}

func (tx *Transaction) Rollback() error {
	if err := tx.recoveryMgr.Rollback(); err != nil {
		return err
	}
	tx.concurMgr.Release()
	tx.mybuffers.unpinAll()
	fmt.Printf("transaction %d rolled back\n", tx.txnum)

	return nil
}

func (tx *Transaction) Recover() error {
	tx.bm.FlushAll(tx.txnum)
	if err := tx.recoveryMgr.Recover(); err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) Pin(blk file.BlockID) error {
	return tx.mybuffers.pin(blk)
}

func (tx *Transaction) Unpin(blk file.BlockID) {
	tx.mybuffers.unpin(blk)
}

func (tx *Transaction) GetInt(blk file.BlockID, offset int32) (int32, error) {
	err := tx.concurMgr.SLock(blk)
	if err != nil {
		return 0, err
	}
	buff := tx.mybuffers.buffers[blk]
	return buff.Contents().GetInt(offset), nil
}

func (tx *Transaction) GetString(blk file.BlockID, offset int32) (string, error) {
	err := tx.concurMgr.SLock(blk)
	if err != nil {
		return "", err
	}
	buff := tx.mybuffers.buffers[blk]
	return buff.Contents().GetString(offset), nil
}

func (tx *Transaction) SetInt(blk file.BlockID, offset, val int32, okToLog bool) error {
	err := tx.concurMgr.XLock(blk)
	if err != nil {
		return err
	}
	buff := tx.mybuffers.buffers[blk]
	var lsn int32 = -1
	if okToLog {
		var err error
		lsn, err = tx.recoveryMgr.SetInt(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetInt(offset, val)
	buff.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *Transaction) SetString(blk file.BlockID, offset int32, val string, okToLog bool) error {
	err := tx.concurMgr.XLock(blk)
	if err != nil {
		return err
	}
	buff := tx.mybuffers.buffers[blk]
	var lsn int32 = -1
	if okToLog {
		var err error
		lsn, err = tx.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *Transaction) Size(filename string) (int32, error) {
	dummyblk := file.NewBlockID(filename, endOfFile)
	if err := tx.concurMgr.SLock(dummyblk); err != nil {
		return 0, err
	}
	return tx.fm.Length(filename)
}

func (tx *Transaction) Append(filename string) (file.BlockID, error) {
	dummyblk := file.NewBlockID(filename, endOfFile)
	if err := tx.concurMgr.XLock(dummyblk); err != nil {
		return file.BlockID{}, err
	}
	return tx.fm.Append(filename)
}

func (tx *Transaction) BlockSize() int32 {
	return tx.fm.BlockSize()
}

func (tx *Transaction) AvailableBuffers() int32 {
	return tx.bm.NumAvailable()
}

func nextTxNumber() int32 {
	txMutex.Lock()
	defer txMutex.Unlock()

	nextTxNum++
	fmt.Printf("new transaction: %d\n", nextTxNum)
	return nextTxNum
}

type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    []file.BlockID
	bm      *buffer.Manager
}

func newBufferList(bm *buffer.Manager) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
}

func (b *BufferList) pin(blk file.BlockID) error {
	buf, err := b.bm.Pin(blk)
	if err != nil {
		return err
	}
	b.buffers[blk] = buf
	b.pins = append(b.pins, blk)
	return nil
}

func (b *BufferList) unpin(blk file.BlockID) {
	buf, ok := b.buffers[blk]
	if !ok {
		panic(fmt.Sprintf("block %v not pinned", blk))
	}
	b.bm.Unpin(buf)
	for i, p := range b.pins {
		if p == blk {
			b.pins = slices.Delete(b.pins, i, i+1)
			break
		}
	}
	if !slices.Contains(b.pins, blk) {
		delete(b.buffers, blk)
	}
}

func (b *BufferList) unpinAll() {
	for _, blk := range b.pins {
		buf, ok := b.buffers[blk]
		if !ok {
			panic(fmt.Sprintf("block %v not pinned", blk))
		}
		b.bm.Unpin(buf)
	}
	b.buffers = make(map[file.BlockID]*buffer.Buffer)
	b.pins = make([]file.BlockID, 0)
}
