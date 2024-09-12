package btree

import (
	"simpledb/file"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchkey   *query.Constant
	contents    *BTreePage
	currentslot int32
	filename    string
}

func NewBTreeLeaf(tx *tx.Transaction, blk file.BlockID, layout *record.Layout, searchkey *query.Constant) (*BTreeLeaf, error) {
	contents, err := NewBTreePage(tx, blk, layout)
	if err != nil {
		return nil, err
	}
	currentslot, err := contents.FindSlotBefore(searchkey)
	if err != nil {
		return nil, err
	}
	return &BTreeLeaf{tx, layout, searchkey, contents, currentslot, blk.FileName}, nil
}

func (bl *BTreeLeaf) Close() error {
	return bl.contents.Close()
}

func (bl *BTreeLeaf) Next() (bool, error) {
	bl.currentslot++
	nRecs, err := bl.contents.GetNumRecs()
	if err != nil {
		return false, err
	}
	if bl.currentslot >= nRecs {
		return bl.tryOverflow()
	}

	val, err := bl.contents.GetDataVal(bl.currentslot)
	if err != nil {
		return false, err
	}
	if val.Equals(bl.searchkey) {
		return true, nil
	} else {
		return bl.tryOverflow()
	}
}

func (bl *BTreeLeaf) GetDataRID() (*record.RID, error) {
	return bl.contents.GetDataRID(bl.currentslot)
}

func (bl *BTreeLeaf) Delete(dataRID *record.RID) error {
	for {
		ok, err := bl.Next()
		if err != nil {
			return err
		}
		// break
		// NOTE: should return error? or just break?
		if !ok {
			return nil
		}
		rid, err := bl.GetDataRID()
		if err != nil {
			return err
		}
		if rid.Equals(dataRID) {
			return bl.contents.Delete(bl.currentslot)
		}
	}
}

func (bl *BTreeLeaf) tryOverflow() (bool, error) {
	firstVal, err := bl.contents.GetDataVal(0)
	if err != nil {
		return false, err
	}
	flag, err := bl.contents.GetFlag()
	if err != nil {
		return false, err
	}
	if !firstVal.Equals(bl.searchkey) || flag < 0 {
		return false, nil
	}
	if err := bl.contents.Close(); err != nil {
		return false, err
	}
	nextBlk := file.NewBlockID(bl.filename, flag)
	contents, err := NewBTreePage(bl.tx, nextBlk, bl.layout)
	if err != nil {
		return false, err
	}
	bl.contents = contents
	bl.currentslot = 0
	return true, nil
}

func (bl *BTreeLeaf) Insert(dataRID *record.RID) (*DirEntry, error) {
	flag, err := bl.contents.GetFlag()
	if err != nil {
		return nil, err
	}
	// If the new record does not fit in the page, split the page and return directory entry for the new page.
	firstVal, err := bl.contents.GetDataVal(0)
	if err != nil {
		return nil, err
	}
	cmp, err := firstVal.CompareTo(bl.searchkey)
	if err != nil {
		return nil, err
	}
	if flag >= 0 && cmp > 0 {
		newBlk, err := bl.contents.Split(0, flag)
		if err != nil {
			return nil, err
		}
		bl.currentslot = 0
		if err := bl.contents.SetFlag(-1); err != nil {
			return nil, err
		}
		if err := bl.contents.InsertLeaf(bl.currentslot, bl.searchkey, dataRID); err != nil {
			return nil, err
		}
		return NewDirEntry(firstVal, newBlk.Number), nil
	}

	bl.currentslot++
	if err := bl.contents.InsertLeaf(bl.currentslot, bl.searchkey, dataRID); err != nil {
		return nil, err
	}
	if isFull, err := bl.contents.IsFull(); err != nil {
		return nil, err
	} else if !isFull {
		return nil, nil
	}

	// if page is full, split it
	firstKey, err := bl.contents.GetDataVal(0)
	if err != nil {
		return nil, err
	}
	nRecs, err := bl.contents.GetNumRecs()
	if err != nil {
		return nil, err
	}
	lastKey, err := bl.contents.GetDataVal(nRecs - 1)
	if err != nil {
		return nil, err
	}
	if lastKey.Equals(firstKey) {
		// create an overflow block to hold all but the first record
		newBlk, err := bl.contents.Split(1, flag)
		if err != nil {
			return nil, err
		}
		if err := bl.contents.SetFlag(newBlk.Number); err != nil {
			return nil, err
		}
		return nil, nil
	} else {
		splitPos := nRecs / 2 // split into half
		splitKey, err := bl.contents.GetDataVal(splitPos)
		if err != nil {
			return nil, err
		}
		if splitKey.Equals(firstKey) {
			// move right, looking for the next key
			for {
				val, err := bl.contents.GetDataVal(splitPos)
				if err != nil {
					return nil, err
				}
				if !val.Equals(splitKey) {
					break
				}
				splitPos++
			}
			splitKey, err = bl.contents.GetDataVal(splitPos)
			if err != nil {
				return nil, err
			}
		} else {
			// move left, looking for first entry having that key
			for {
				val, err := bl.contents.GetDataVal(splitPos - 1)
				if err != nil {
					return nil, err
				}
				if !val.Equals(splitKey) {
					break
				}
				splitPos--
			}
		}
		newBlk, err := bl.contents.Split(splitPos, -1)
		if err != nil {
			return nil, err
		}
		return NewDirEntry(splitKey, newBlk.Number), nil
	}
}
