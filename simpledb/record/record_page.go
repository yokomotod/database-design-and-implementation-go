package record

import (
	"simpledb/file"
	"simpledb/tx"
	"simpledb/util/logger"
)

type InUseFlag int32

const (
	Empty InUseFlag = 0
	Used  InUseFlag = 1
)

type RecordPage struct {
	logger *logger.Logger

	tx     *tx.Transaction
	blk    file.BlockID
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, blk file.BlockID, layout *Layout) (*RecordPage, error) {
	logger := logger.New("record.RecordPage", logger.Trace)

	logger.Tracef("NewRecordPage(): tx.Pin(%+v)", blk)
	if err := tx.Pin(blk); err != nil {
		return nil, err
	}
	return &RecordPage{logger, tx, blk, layout}, nil
}

func (rp *RecordPage) GetInt(slot int32, fieldName string) (int32, error) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.GetInt(rp.blk, fieldPos)
}

func (rp *RecordPage) GetString(slot int32, fieldName string) (string, error) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.GetString(rp.blk, fieldPos)
}

func (rp *RecordPage) SetInt(slot int32, fieldName string, val int32) error {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.SetInt(rp.blk, fieldPos, val, true)
}

func (rp *RecordPage) SetString(slot int32, fieldName string, val string) error {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	return rp.tx.SetString(rp.blk, fieldPos, val, true)
}

func (rp *RecordPage) Delete(slot int32) error {
	return rp.setFlag(slot, Empty)
}

// Format 新しいブロックを作成する
// ブロックを作成する時に書き込む値は無意味なため、logには書き込まないようにする
func (rp *RecordPage) Format() error {
	var slot int32 = 0
	for rp.isValidSlot(slot) {
		err := rp.tx.SetInt(rp.blk, rp.offset(slot), int32(Empty), false)
		if err != nil {
			return err
		}
		schema := rp.layout.Schema()
		for _, fieldName := range schema.Fields() {
			fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
			if schema.Type(fieldName) == INT {
				err = rp.tx.SetInt(rp.blk, fieldPos, 0, false)
			} else {
				err = rp.tx.SetString(rp.blk, fieldPos, "", false)
			}
			if err != nil {
				return err
			}
		}
		slot++
	}
	return nil
}

func (rp *RecordPage) NextAfter(slot int32) (int32, error) {
	return rp.searchAfter(slot, Used)
}

// InsertAfter 指定されたスロットの後に新しいレコードを挿入する(inuseフラグを立てる)
func (rp *RecordPage) InsertAfter(slot int32) (int32, error) {
	newSlot, err := rp.searchAfter(slot, Empty)
	if err != nil {
		return 0, err
	}
	if newSlot >= 0 {
		if err := rp.setFlag(newSlot, Used); err != nil {
			return 0, err
		}
	}
	return newSlot, nil
}

func (rp *RecordPage) Block() file.BlockID {
	return rp.blk
}

func (rp *RecordPage) setFlag(slot int32, flag InUseFlag) error {
	return rp.tx.SetInt(rp.blk, rp.offset(slot), int32(flag), true)
}

func (rp *RecordPage) searchAfter(slot int32, flag InUseFlag) (int32, error) {
	slot++
	for rp.isValidSlot(slot) {
		slotFlag, err := rp.tx.GetInt(rp.blk, rp.offset(slot))
		if err != nil {
			return 0, err
		}
		if InUseFlag(slotFlag) == flag {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.SlotSize()
}
