package query

import (
	"fmt"
	"simpledb/file"
	"simpledb/record"
	"simpledb/tx"
)

var _ Scan = (*ChunkScan)(nil)

type ChunkScan struct {
	buffs                                 []*record.RecordPage
	tx                                    *tx.Transaction
	filename                              string
	layout                                *record.Layout
	startBlkNum, endBlkNum, currentBlkNum int32
	rp                                    *record.RecordPage
	currentSlot                           int32
}

func NewChunkScan(tx *tx.Transaction, filename string, layout *record.Layout, startBlkNum, endBlkNum int32) (*ChunkScan, error) {
	buffs := make([]*record.RecordPage, 0, endBlkNum-startBlkNum+1)
	for i := startBlkNum; i <= endBlkNum; i++ {
		blk := file.NewBlockID(filename, i)
		rp, err := record.NewRecordPage(tx, blk, layout)
		if err != nil {
			return nil, fmt.Errorf("record.NewRecordPage: %w", err)
		}
		buffs = append(buffs, rp)
	}

	s := &ChunkScan{
		buffs:       buffs,
		tx:          tx,
		filename:    filename,
		layout:      layout,
		startBlkNum: startBlkNum,
		endBlkNum:   endBlkNum,
	}

	s.moveToBlock(startBlkNum)

	return s, nil
}

func (s *ChunkScan) Close() {
	for i := range s.buffs {
		s.tx.Unpin(file.NewBlockID(s.filename, s.startBlkNum+int32(i)))
	}
}

func (s *ChunkScan) BeforeFirst() error {
	s.moveToBlock(s.startBlkNum)

	return nil
}

func (s *ChunkScan) Next() (bool, error) {
	var err error
	s.currentSlot, err = s.rp.NextAfter(s.currentSlot)
	if err != nil {
		return false, fmt.Errorf("s.rp.NextAfter: %w", err)
	}

	for s.currentSlot < 0 {
		if s.currentBlkNum == s.endBlkNum {
			return false, nil
		}
		s.moveToBlock(s.currentBlkNum + 1)
		s.currentSlot, err = s.rp.NextAfter(s.currentSlot)
		if err != nil {
			return false, fmt.Errorf("s.rp.NextAfter: %w", err)
		}
	}

	return true, nil
}

func (s *ChunkScan) GetInt(fldname string) (int32, error) {
	return s.rp.GetInt(s.currentSlot, fldname)
}

func (s *ChunkScan) GetString(fldname string) (string, error) {
	return s.rp.GetString(s.currentSlot, fldname)
}

func (s *ChunkScan) GetVal(fldname string) (*Constant, error) {
	if s.layout.Schema().Type(fldname) == record.INT {
		i, err := s.GetInt(fldname)
		if err != nil {
			return nil, fmt.Errorf("s.GetInt: %w", err)
		}
		return NewConstantWithInt(i), nil
	}

	if s.layout.Schema().Type(fldname) == record.VARCHAR {
		s, err := s.GetString(fldname)
		if err != nil {
			return nil, fmt.Errorf("s.GetString: %w", err)
		}
		return NewConstantWithString(s), nil
	}

	return nil, fmt.Errorf("unknown value type: %d", s.layout.Schema().Type(fldname))
}

func (s *ChunkScan) HasField(fldname string) bool {
	return s.layout.Schema().HasField(fldname)
}

func (s *ChunkScan) moveToBlock(blkNum int32) {
	s.currentBlkNum = blkNum
	s.rp = s.buffs[s.currentBlkNum-s.startBlkNum]
	s.currentSlot = -1
}
