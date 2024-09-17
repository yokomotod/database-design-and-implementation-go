package btree

import (
	"fmt"
	"simpledb/file"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

type BTreePage struct {
	tx             *tx.Transaction
	currentBlockID file.BlockID
	layout         *record.Layout
}

func NewBTreePage(tx *tx.Transaction, currentBlockID file.BlockID, layout *record.Layout) (*BTreePage, error) {
	if err := tx.Pin(currentBlockID); err != nil {
		return nil, err
	}
	return &BTreePage{tx, currentBlockID, layout}, nil
}

func (bp *BTreePage) FindSlotBefore(searchKey *query.Constant) (int32, error) {
	var slot int32 = 0
	for {
		nRecs, err := bp.GetNumRecs()
		if err != nil {
			return 0, err
		}
		if slot == nRecs {
			break
		}
		val, err := bp.GetDataVal(slot)
		if err != nil {
			return 0, err
		}
		cnp, err := val.CompareTo(searchKey)
		if err != nil {
			return 0, err
		}
		if cnp >= 0 {
			break
		}
		slot++
	}
	return slot - 1, nil
}

func (bp *BTreePage) Close() error {
	emptyBlockID := file.BlockID{}
	if bp.currentBlockID == emptyBlockID {
		return nil
	}
	bp.tx.Unpin(bp.currentBlockID)
	bp.currentBlockID = emptyBlockID
	return nil
}

func (bp *BTreePage) IsFull() (bool, error) {
	nRecs, err := bp.GetNumRecs()
	if err != nil {
		return false, err
	}
	return bp.slotPos(nRecs+1) >= bp.tx.BlockSize(), nil
}

func (bp *BTreePage) Split(splitPos int32, flag int32) (file.BlockID, error) {
	newBlockID, err := bp.appendNew(flag)
	if err != nil {
		return file.BlockID{}, err
	}
	newPage, err := NewBTreePage(bp.tx, newBlockID, bp.layout)
	if err != nil {
		return file.BlockID{}, err
	}
	defer newPage.Close()
	if err := bp.transferRecs(splitPos, newPage); err != nil {
		return file.BlockID{}, err
	}
	if err := newPage.SetFlag(flag); err != nil {
		return file.BlockID{}, err
	}
	return newBlockID, nil
}

func (bp *BTreePage) GetNumRecs() (int32, error) {
	return bp.tx.GetInt(bp.currentBlockID, file.Int32Bytes)
}

func (bp *BTreePage) GetDataVal(slot int32) (*query.Constant, error) {
	return bp.getVal(slot, "dataval")
}

func (bp *BTreePage) GetFlag() (int32, error) {
	return bp.tx.GetInt(bp.currentBlockID, 0)
}

func (bp *BTreePage) SetFlag(val int32) error {
	return bp.tx.SetInt(bp.currentBlockID, 0, val, true)
}

func (bp *BTreePage) getVal(slot int32, fieldName string) (*query.Constant, error) {
	valType := bp.layout.Schema().Type(fieldName)
	switch valType {
	case record.INT:
		intVal, err := bp.tx.GetInt(bp.currentBlockID, bp.fieldPos(slot, fieldName))
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithInt(intVal), nil
	case record.VARCHAR:
		strVal, err := bp.tx.GetString(bp.currentBlockID, bp.fieldPos(slot, fieldName))
		if err != nil {
			return nil, err
		}
		return query.NewConstantWithString(strVal), nil
	default:
		return nil, fmt.Errorf("unexpected value type: %d", valType)
	}
}

func (bp *BTreePage) setInt(slot int32, fieldName string, val int32) error {
	return bp.tx.SetInt(bp.currentBlockID, bp.fieldPos(slot, fieldName), val, true)
}

func (bp *BTreePage) setString(slot int32, fieldName string, val string) error {
	return bp.tx.SetString(bp.currentBlockID, bp.fieldPos(slot, fieldName), val, true)
}

func (bp *BTreePage) setVal(slot int32, fieldName string, val *query.Constant) error {
	valType := bp.layout.Schema().Type(fieldName)
	switch valType {
	case record.INT:
		valInt, err := val.AsInt()
		if err != nil {
			return err
		}
		return bp.setInt(slot, fieldName, valInt)
	case record.VARCHAR:
		valStr, err := val.AsString()
		if err != nil {
			return err
		}
		return bp.setString(slot, fieldName, valStr)
	default:
		return fmt.Errorf("unexpected value type: %d", valType)
	}
}

func (bp *BTreePage) setNumRecs(n int32) error {
	return bp.tx.SetInt(bp.currentBlockID, file.Int32Bytes, n, true)
}

func (bp *BTreePage) insert(slot int32) error {
	nRecs, err := bp.GetNumRecs()
	if err != nil {
		return err
	}
	for i := nRecs; i > slot; i-- {
		if err := bp.copyRecord(i-1, i); err != nil {
			return err
		}
	}
	if err := bp.setNumRecs(nRecs + 1); err != nil {
		return err
	}
	return nil
}

func (bp *BTreePage) copyRecord(from, to int32) error {
	for _, fieldName := range bp.layout.Schema().Fields() {
		val, err := bp.getVal(from, fieldName)
		if err != nil {
			return err
		}
		if err := bp.setVal(to, fieldName, val); err != nil {
			return err
		}
	}
	return nil
}

func (bp *BTreePage) fieldPos(slot int32, fieldName string) int32 {
	return bp.slotPos(slot) + bp.layout.Offset(fieldName)
}

func (bp *BTreePage) slotPos(slot int32) int32 {
	return file.Int32Bytes + file.Int32Bytes*(slot*bp.layout.SlotSize())
}

func (bp *BTreePage) appendNew(flag int32) (file.BlockID, error) {
	newBlockID, err := bp.tx.Append((bp.currentBlockID).FileName)
	if err != nil {
		return file.BlockID{}, err
	}
	if err := bp.tx.Pin(newBlockID); err != nil {
		return file.BlockID{}, err
	}
	if err := bp.Format(newBlockID, flag); err != nil {
		return file.BlockID{}, err
	}
	return newBlockID, nil
}

func (bp *BTreePage) Format(blk file.BlockID, flag int32) error {
	if err := bp.tx.SetInt(blk, 0, flag, false); err != nil {
		return err
	}
	if err := bp.tx.SetInt(blk, file.Int32Bytes, 0, false); err != nil {
		return err
	}
	for pos := 2 * file.Int32Bytes; pos+bp.layout.SlotSize() <= bp.tx.BlockSize(); pos += bp.layout.SlotSize() {
		if err := bp.makeDefaultRecord(blk, pos); err != nil {
			return err
		}
	}
	return nil
}

func (bp *BTreePage) makeDefaultRecord(blk file.BlockID, pos int32) error {
	for _, fieldName := range bp.layout.Schema().Fields() {
		offset := bp.layout.Offset(fieldName)
		switch bp.layout.Schema().Type(fieldName) {
		case record.INT:
			if err := bp.tx.SetInt(blk, pos+offset, 0, false); err != nil {
				return err
			}
		case record.VARCHAR:
			if err := bp.tx.SetString(blk, pos+offset, "", false); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected value type: %d", bp.layout.Schema().Type(fieldName))
		}
	}
	return nil
}

func (bp *BTreePage) transferRecs(slot int32, dest *BTreePage) error {
	destSlot := int32(0)
	for {
		nRecs, err := bp.GetNumRecs()
		if err != nil {
			return err
		}
		if slot >= nRecs {
			break
		}
		if err := dest.insert(destSlot); err != nil {
			return err
		}
		for _, fieldName := range dest.layout.Schema().Fields() {
			val, err := bp.getVal(slot, fieldName)
			if err != nil {
				return err
			}
			if err := dest.setVal(destSlot, fieldName, val); err != nil {
				return err
			}
		}
		if err := bp.Delete(slot); err != nil {
			return err
		}
		destSlot++
	}
	return nil
}

func (bp *BTreePage) Delete(slot int32) error {
	nRecs, err := bp.GetNumRecs()
	if err != nil {
		return err
	}
	for i := slot + 1; i < nRecs; i++ {
		if err := bp.copyRecord(i, i-1); err != nil {
			return err
		}
	}
	if err := bp.setNumRecs(nRecs - 1); err != nil {
		return err
	}
	return nil
}

// Methods called only by BTreeDir
func (bp *BTreePage) GetChildNum(slot int32) (int32, error) {
	return bp.getInt(slot, "block")
}

func (bp *BTreePage) InsertDir(slot int32, val *query.Constant, blkNum int32) error {
	if err := bp.insert(slot); err != nil {
		return err
	}
	if err := bp.setVal(slot, "dataval", val); err != nil {
		return err
	}
	if err := bp.setInt(slot, "block", blkNum); err != nil {
		return err
	}
	return nil
}

func (bp *BTreePage) GetDataRID(slot int32) (*record.RID, error) {
	blockNum, err := bp.getInt(slot, "block")
	if err != nil {
		return nil, err
	}
	id, err := bp.getInt(slot, "id")
	if err != nil {
		return nil, err
	}
	return record.NewRID(blockNum, id), nil
}

func (bp *BTreePage) InsertLeaf(slot int32, val *query.Constant, rid *record.RID) error {
	if err := bp.insert(slot); err != nil {
		return err
	}
	if err := bp.setVal(slot, "dataval", val); err != nil {
		return err
	}
	if err := bp.setInt(slot, "block", rid.BlockNumber()); err != nil {
		return err
	}
	if err := bp.setInt(slot, "id", rid.Slot()); err != nil {
		return err
	}
	return nil
}

func (bp *BTreePage) getInt(slot int32, fieldName string) (int32, error) {
	pos := bp.fieldPos(slot, fieldName)
	return bp.tx.GetInt(bp.currentBlockID, pos)
}
