package recovery

import (
	"fmt"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
)

type Transaction interface {
	Pin(blockID *file.BlockID) error
	SetString(blockID *file.BlockID, offset int32, val string, logRecord bool) error
	SetInt(blockID *file.BlockID, offset int32, val int32, logRecord bool) error
	Unpin(blockID *file.BlockID)
}

type Manager struct {
	logMgr      *log.Manager
	bufferMgr   *buffer.Manager
	transaction Transaction
	txnum       int32
}

func New(tx Transaction, txnum int32, logMgr *log.Manager, bufMgr *buffer.Manager) (*Manager, error) {
	if _, err := newStartRecord(txnum).WriteToLog(logMgr); err != nil {
		return nil, err
	}
	return &Manager{
		logMgr:      logMgr,
		bufferMgr:   bufMgr,
		transaction: tx,
		txnum:       txnum,
	}, nil
}

func (m *Manager) Commit() error {
	m.bufferMgr.FlushAll(m.txnum)
	lsn, err := newCommitRecord(m.txnum).WriteToLog(m.logMgr)
	if err != nil {
		return err
	}
	m.logMgr.Flush(lsn)
	return nil
}

func (m *Manager) Rollback() error {
	err := m.doRollback()
	if err != nil {
		return err
	}
	m.bufferMgr.FlushAll(m.txnum)
	lsn, err := newRollbackRecord(m.txnum).WriteToLog(m.logMgr)
	if err != nil {
		return err
	}
	m.logMgr.Flush(lsn)
	return nil
}

func (m *Manager) Recover() error {
	err := m.doRecover()
	if err != nil {
		return err
	}

	m.bufferMgr.FlushAll(m.txnum)
	lsn, err := newCheckPointRecord().WriteToLog(m.logMgr)
	if err != nil {
		return err
	}
	m.logMgr.Flush(lsn)
	return nil
}

func (m *Manager) SetInt(buf *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	oldVal := buf.Contents().GetInt(offset)
	blk := buf.Block()
	return newSetIntRecord(m.txnum, blk, offset, oldVal).WriteToLog(m.logMgr)
}

func (m *Manager) SetString(buf *buffer.Buffer, offset int32, newVal string) (int32, error) {
	oldVal := buf.Contents().GetString(offset)
	blk := buf.Block()
	return newSetStringRecord(m.txnum, blk, offset, oldVal).WriteToLog(m.logMgr)
}

func (m *Manager) doRollback() error {
	it, err := m.logMgr.Iterator()
	if err != nil {
		return fmt.Errorf("recovery.doRollback: %w", err)
	}
	for it.HasNext() {
		bytes, err := it.Next()
		if err != nil {
			return fmt.Errorf("recovery.doRollback: %w", err)
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("recovery.doRollback for %s: %w", string(bytes), err)
		}
		if rec.TxNumber() == m.txnum {
			if rec.Op() == Start {
				return nil
			}
			if err := rec.Undo(m.transaction); err != nil {
				return fmt.Errorf("Undo: %w", err)
			}
		}
	}
	return nil
}

func (m *Manager) doRecover() error {
	finishedTx := make(map[int32]struct{})
	it, err := m.logMgr.Iterator()
	if err != nil {
		return fmt.Errorf("recovery.doRecover: %w", err)
	}
	for it.HasNext() {
		bytes, err := it.Next()
		if err != nil {
			return fmt.Errorf("recovery.doRecover: %w", err)
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("recovery.doRecover for %s: %w", string(bytes), err)
		}
		if rec.Op() == CheckPoint {
			return nil
		} else if rec.Op() == Commit || rec.Op() == Rollback {
			finishedTx[rec.TxNumber()] = struct{}{}
		} else if _, ok := finishedTx[rec.TxNumber()]; !ok {
			if err := rec.Undo(m.transaction); err != nil {
				return fmt.Errorf("Undo: %w", err)
			}
		}
	}
	return nil
}
