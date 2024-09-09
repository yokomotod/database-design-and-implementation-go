package query

import (
	"errors"
	"simpledb/file"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
)

var ErrUnkownFieldType = errors.New("unknown field type")

var _ Scan = (*TableScan)(nil)
var _ UpdateScan = (*TableScan)(nil)

type TableScan struct {
	logger *logger.Logger

	tx          *tx.Transaction
	layout      *record.Layout
	rp          *record.RecordPage
	filename    string
	currentSlot int32
	TotalBlkNum int32
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *record.Layout) (*TableScan, error) {
	logger := logger.New("query.TableScan", logger.Trace)

	filename := tableName + ".tbl"
	tableScan := &TableScan{logger, tx, layout, nil, filename, -1, 0}

	logger.Debugf("(%q) NewTableScan(): tx.Size(%q)", filename, tableScan.filename)
	size, err := tx.Size(tableScan.filename)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		logger.Debugf("(%q) NewTableScan(): size=0, moveToNewBlock()", filename)
		if err := tableScan.moveToNewBlock(); err != nil {
			return nil, err
		}
	} else {
		logger.Debugf("(%q) NewTableScan(): size=%d, moveToBlock(0)", filename, size)
		if err := tableScan.moveToBlock(0); err != nil {
			return nil, err
		}
	}

	return tableScan, nil
}

func (ts *TableScan) BeforeFirst() error {
	if err := ts.moveToBlock(0); err != nil {
		return err
	}

	return nil
}

func (ts *TableScan) Next() (bool, error) {
	var err error
	for {
		ts.logger.Tracef("(%q) Next(): rp.NextAfter(%d)", ts.filename, ts.currentSlot)
		ts.currentSlot, err = ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
			return false, err
		}
		if ts.currentSlot >= 0 {
			break
		}
		atLastBlock, err := ts.AtLastBlock()
		if err != nil {
			return false, err
		}
		if atLastBlock {
			return false, nil
		}
		if err := ts.moveToBlock(ts.rp.Block().Number + 1); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (ts *TableScan) GetInt(fieldName string) (int32, error) {
	return ts.rp.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetVal(fieldName string) (*Constant, error) {
	switch ts.layout.Schema().Type(fieldName) {
	case record.INT:
		val, err := ts.GetInt(fieldName)
		if err != nil {
			return nil, err
		}
		return NewConstantWithInt(val), nil
	case record.VARCHAR:
		val, err := ts.GetString(fieldName)
		if err != nil {
			return nil, err
		}
		return NewConstantWithString(val), nil
	default:
		return nil, ErrUnkownFieldType
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.logger.Debugf("(%q) Close(): Unpin(%+v)", ts.filename, ts.rp.Block())
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScan) SetInt(fieldName string, val int32) error {
	return ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val *Constant) error {
	switch ts.layout.Schema().Type(fieldName) {
	case record.INT:
		ival, err := val.AsInt()
		if err != nil {
			return err
		}
		if err := ts.SetInt(fieldName, ival); err != nil {
			return err
		}
	case record.VARCHAR:
		sval, err := val.AsString()
		if err != nil {
			return err
		}
		if err := ts.SetString(fieldName, sval); err != nil {
			return err
		}
	default:
		return ErrUnkownFieldType
	}
	return nil
}

func (ts *TableScan) CanInsertCurrentBlock() (bool, error) {
	newSlot, err := ts.rp.SearchAfter(ts.currentSlot, record.Empty)
	if err != nil {
		return false, err
	}

	return newSlot >= 0, nil
}

func (ts *TableScan) Insert() error {
	nextSlot, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return err
	}
	ts.logger.Tracef("(%q) Insert(): currentSlot=%d, nextSlot=%d", ts.filename, ts.currentSlot, nextSlot)
	ts.currentSlot = nextSlot
	for ts.currentSlot < 0 {
		ts.logger.Debugf("(%q) Insert(): nextSlot=%d", ts.filename, nextSlot)
		atLastBlock, err := ts.AtLastBlock()
		if err != nil {
			return err
		}
		if atLastBlock {
			ts.logger.Debugf("(%q) Insert(): atLastBlock=true, moveToNewBlock()", ts.filename)
			if err := ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			ts.logger.Debugf("(%q) Insert(): atLastBlock=false, moveToBlock(%d)", ts.filename, ts.rp.Block().Number+1)
			if err := ts.moveToBlock(ts.rp.Block().Number + 1); err != nil {
				return err
			}
		}

		nextSlot, err = ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return err
		}
		ts.currentSlot = nextSlot
	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRID(rid *record.RID) (err error) {
	ts.Close()
	block := file.NewBlockID(ts.filename, rid.BlockNumber())
	ts.rp, err = record.NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.currentSlot = rid.Slot()

	return nil
}

func (ts *TableScan) moveToBlock(blockNum int32) (err error) {
	ts.Close()
	block := file.NewBlockID(ts.filename, blockNum)
	ts.rp, err = record.NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.currentSlot = -1

	return nil
}

func (ts *TableScan) GetRID() (*record.RID, error) {
	return record.NewRID(ts.rp.Block().Number, ts.currentSlot), nil
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	blockId, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	ts.rp, err = record.NewRecordPage(ts.tx, blockId, ts.layout)
	if err != nil {
		return err
	}
	err = ts.rp.Format()
	if err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) AtLastBlock() (bool, error) {
	ts.logger.Debugf("(%q) AtLastBlock(): tx.Size", ts.filename)
	fileSize, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, err
	}
	return ts.rp.Block().Number == fileSize-1, nil
}
