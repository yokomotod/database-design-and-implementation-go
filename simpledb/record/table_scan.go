package record

import (
	"simpledb/file"
	"simpledb/tx"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentSlot int32
}

// 本当はUpdateScanのインターフェースを満たすべきだが、UpdateScanのメソッドは未実装なので今は無視
func NewTableScan(tx *tx.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	tableScan := &TableScan{tx, layout, nil, tableName + ".tbl", -1}
	size, err := tx.Size(tableScan.filename)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		err = tableScan.moveToNewBlock()
	} else {
		tableScan.moveToBlock(0)
	}
	if err != nil {
		return nil, err
	}
	return tableScan, nil
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() (bool, error) {
	var err error
	ts.currentSlot, err = ts.rp.NextAfter(ts.currentSlot)
	if err != nil {
		return false, err
	}
	for ts.currentSlot < 0 {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLastBlock {
			return false, nil
		}
		ts.moveToBlock(ts.rp.Block().Number + 1)
		ts.currentSlot, err = ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
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

func (ts *TableScan) GetVal(fieldName string) (interface{}, error) {
	if ts.layout.Schema().Type(fieldName) == INT {
		return ts.GetInt(fieldName)
	} else {
		return ts.GetString(fieldName)
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScan) SetInt(fieldName string, val int32) error {
	return ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val interface{}) {
	if ts.layout.Schema().Type(fieldName) == INT {
		ts.SetInt(fieldName, val.(int32))
	} else {
		ts.SetString(fieldName, val.(string))
	}
}

func (ts *TableScan) Insert() error {
	nextSlot, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return err
	}
	ts.currentSlot = nextSlot
	for ts.currentSlot < 0 {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return err
		}
		if atLastBlock {
			err = ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().Number + 1)
		}
		if err != nil {
			return err
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

func (ts *TableScan) MoveToRID(rid *RID) {
	ts.Close()
	block := file.NewBlockID(ts.filename, rid.BlockNumber())
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = rid.Slot()
}

func (ts *TableScan) moveToBlock(blockNum int32) {
	ts.Close()
	block := file.NewBlockID(ts.filename, blockNum)
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = -1
}

func (ts *TableScan) GetRID() *RID {
	return NewRID(ts.rp.Block().Number, ts.currentSlot)
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	blockId, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	ts.rp = NewRecordPage(ts.tx, blockId, ts.layout)
	err = ts.rp.Format()
	if err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, err
	}
	return ts.rp.Block().Number == fileSize-1, nil
}
