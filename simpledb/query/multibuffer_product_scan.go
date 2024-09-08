package query

import (
	"fmt"
	"simpledb/record"
	"simpledb/tx"
)

var _ Scan = (*MultibufferProductScan)(nil)

type MultibufferProductScan struct {
	tx         *tx.Transaction
	lhs        Scan
	rhs        *ChunkScan   // 元実装では `Scan`
	prod       *ProductScan // 元実装では `Scan`
	filename   string
	layout     *record.Layout
	chunkSize  int32
	nextBlkNum int32
	fileSize   int32
}

func NewMultibufferProductScan(tx *tx.Transaction, lhs Scan, tableName string, layout *record.Layout) (*MultibufferProductScan, error) {
	fileName := tableName + ".tbl"

	fileSize, err := tx.Size(fileName)
	if err != nil {
		return nil, fmt.Errorf("tx.Size: %w", err)
	}

	available := tx.AvailableBuffers()
	chunkSize := BufferNeedsBestFactor(available, fileSize)

	return &MultibufferProductScan{
		tx:        tx,
		lhs:       lhs,
		filename:  fileName,
		layout:    layout,
		chunkSize: chunkSize,
	}, nil
}

func (s *MultibufferProductScan) BeforeFirst() error {
	s.nextBlkNum = 0
	_, err := s.useNextChunk()
	if err != nil {
		return fmt.Errorf("s.useNextChunk: %w", err)
	}

	return nil
}

/**
 * Moves to the next record in the current scan.
 * If there are no more records in the current chunk,
 * then move to the next LHS record and the beginning of that chunk.
 * If there are no more LHS records, then move to the next chunk
 * and begin again.
 * @see simpledb.query.Scan#next()
 */
func (s *MultibufferProductScan) Next() (bool, error) {
	for {
		next, err := s.prod.Next()
		if err != nil {
			return false, fmt.Errorf("s.prod.Next: %w", err)
		}

		if next {
			return true, nil
		}

		ok, err := s.useNextChunk()
		if err != nil {
			return false, fmt.Errorf("s.useNextChunk: %w", err)
		}

		if !ok {
			return false, nil
		}
	}
}

func (s *MultibufferProductScan) Close() {
	s.prod.Close()
}

func (s *MultibufferProductScan) GetVal(fldname string) (*Constant, error) {
	return s.prod.GetVal(fldname)
}

func (s *MultibufferProductScan) GetInt(fldname string) (int32, error) {
	return s.prod.GetInt(fldname)
}

func (s *MultibufferProductScan) GetString(fldname string) (string, error) {
	return s.prod.GetString(fldname)
}

func (s *MultibufferProductScan) HasField(fldname string) bool {
	return s.prod.HasField(fldname)
}

func (s *MultibufferProductScan) useNextChunk() (bool, error) {
	if s.nextBlkNum >= s.fileSize {
		return false, nil
	}

	if s.rhs != nil {
		s.rhs.Close()
	}

	end := s.nextBlkNum + s.chunkSize - 1
	if end >= s.fileSize {
		end = s.fileSize - 1
	}

	rhs, err := NewChunkScan(s.tx, s.filename, s.layout, s.nextBlkNum, end)
	if err != nil {
		return false, fmt.Errorf("NewChunkScan: %w", err)
	}

	if err := s.lhs.BeforeFirst(); err != nil {
		return false, fmt.Errorf("s.lhs.BeforeFirst: %w", err)
	}

	s.prod, err = NewProductScan(s.lhs, rhs)
	if err != nil {
		return false, fmt.Errorf("NewProductScan: %w", err)
	}

	s.nextBlkNum = end + 1

	return true, nil
}
