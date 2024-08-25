package btree

import (
	"fmt"
	"math"
	"simpledb/file"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

type BTreeIndex struct {
	tx         *tx.Transaction
	dirLayout  *record.Layout
	leafLayout *record.Layout
	leaftbl    string
	leaf       *BTreeLeaf
	rootblk    file.BlockID
}

func NewBTreeIndex(
	tx *tx.Transaction,
	idxName string,
	leafLayout *record.Layout,
) (*BTreeIndex, error) {
	leafTable := idxName + "leaf"
	if size, err := tx.Size(leafTable); err != nil {
		return nil, err
	} else if size == 0 {
		blk, err := tx.Append(leafTable)
		if err != nil {
			return nil, err
		}
		node, err := NewBTreePage(tx, blk, leafLayout)
		if err != nil {
			return nil, err
		}
		if err := node.Format(blk, -1); err != nil {
			return nil, err
		}
	}

	dirSchema := record.NewSchema()
	dirSchema.Add("block", leafLayout.Schema())
	dirSchema.Add("dataval", leafLayout.Schema())
	dirTable := idxName + "dir"
	dirLayout := record.NewLayoutFromSchema(dirSchema)
	rootblk := file.NewBlockID(dirTable, 0)
	if size, err := tx.Size(dirTable); err != nil {
		return nil, err
	} else if size == 0 {
		if _, err := tx.Append(dirTable); err != nil {
			return nil, err
		}
		node, err := NewBTreePage(tx, rootblk, dirLayout)
		if err != nil {
			return nil, err
		}
		if err := node.Format(rootblk, 0); err != nil {
			return nil, err
		}
		fldtype := dirSchema.Type("dataval")
		var minval *query.Constant
		switch fldtype {
		case record.INT:
			minval = query.NewConstantWithInt(math.MinInt32)
		case record.VARCHAR:
			minval = query.NewConstantWithString("")
		default:
			return nil, fmt.Errorf("unexpected value type: %d", fldtype)
		}
		if err := node.InsertDir(0, minval, 0); err != nil {
			return nil, err
		}
		if err := node.Close(); err != nil {
			return nil, err
		}
	}

	return &BTreeIndex{
		tx:         tx,
		dirLayout:  dirLayout,
		leafLayout: leafLayout,
		leaftbl:    leafTable,
		rootblk:    rootblk,
	}, nil
}

func (bi *BTreeIndex) BeforeFirst(searchKey *query.Constant) error {
	bi.Close()
	root, err := NewBTreeDir(bi.tx, bi.rootblk, bi.dirLayout)
	if err != nil {
		return err
	}
	blknum, err := root.Search(searchKey)
	if err != nil {
		return err
	}
	if err := root.Close(); err != nil {
		return err
	}
	blk := file.NewBlockID(bi.leaftbl, blknum)
	leaf, err := NewBTreeLeaf(bi.tx, blk, bi.leafLayout, searchKey)
	if err != nil {
		return err
	}
	bi.leaf = leaf
	return nil
}

func (bi *BTreeIndex) Next() (bool, error) {
	return bi.leaf.Next()
}

func (bi *BTreeIndex) GetDataRID() (*record.RID, error) {
	return bi.leaf.GetDataRID()
}

func (bi *BTreeIndex) Insert(dataVal *query.Constant, dataRID *record.RID) error {
	if err := bi.BeforeFirst(dataVal); err != nil {
		return err
	}
	entry, err := bi.leaf.Insert(dataRID)
	if err != nil {
		return err
	}
	if err := bi.leaf.Close(); err != nil {
		return err
	}
	if entry == nil {
		return nil
	}
	root, err := NewBTreeDir(bi.tx, bi.rootblk, bi.dirLayout)
	if err != nil {
		return err
	}
	entry2, err := root.Insert(entry)
	if err != nil {
		return err
	}
	if entry2 != nil {
		if err := root.MakeNewRoot(entry2); err != nil {
			return err
		}
	}
	if err := root.Close(); err != nil {
		return err
	}
	return nil
}

func (bi *BTreeIndex) Delete(dataVal *query.Constant, dataRID *record.RID) error {
	if err := bi.BeforeFirst(dataVal); err != nil {
		return err
	}
	if err := bi.leaf.Delete(dataRID); err != nil {
		return err
	}
	if err := bi.leaf.Close(); err != nil {
		return err
	}
	return nil
}

func (bi *BTreeIndex) Close() error {
	if bi.leaf != nil {
		return bi.leaf.Close()
	}
	return nil
}

func SearchCost(numBlocks, rpb int) int {
	return 1 + int(math.Log(float64(numBlocks))/math.Log(float64(rpb)))
}
