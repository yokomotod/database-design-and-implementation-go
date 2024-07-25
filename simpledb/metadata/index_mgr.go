package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

type IndexInfo struct {
	idxname   string
	fldname   string
	tx        *tx.Transaction
	tblSchema *record.Schema
	idxLayout *record.Layout
	si        *StatInfo
}

func NewIndexInfo(idxname string, fldname string, tblSchema *record.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {
	ii := &IndexInfo{idxname, fldname, tx, tblSchema, nil, si}
	ii.idxLayout = ii.createIdxLayout()
	return ii
}

// TODO: HashIndex が作られた際に要再実装
// func (ii *IndexInfo) Open() *Index {
// 	return NewHashIndex(ii.tx, ii.idxname, ii.idxLayout)
// }
//
// func (ii *IndexInfo) BlocksAccessed() int {
// 	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
// 	numblocks := ii.si.RecordsOutput() / rpb
// 	return HashIndex.SearchCost(numblocks, rpb)
// }

func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldname)
}

func (ii *IndexInfo) DistinctValues(fname string) int {
	var result int
	if ii.fldname == fname {
		result = 1
	} else {
		result = ii.si.DistinctValues(ii.fldname)
	}
	return result
}

func (ii *IndexInfo) createIdxLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if ii.tblSchema.Type(ii.fldname) == record.INT {
		schema.AddIntField("dataval")
	} else {
		fldlen := ii.tblSchema.Length(ii.fldname)
		schema.AddStringField("dataval", fldlen)
	}
	return record.NewLayoutFromSchema(schema)
}

type IndexMgr struct {
	layout  *record.Layout
	tblMgr  *TableMgr
	statMgr *StatMgr
}

func NewIndexMgr(isNew bool, tblMgr *TableMgr, statMgr *StatMgr, tx *tx.Transaction) (*IndexMgr, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("indexname", MaxName)
		schema.AddStringField("tablename", MaxName)
		schema.AddStringField("fieldname", MaxName)
		err := tblMgr.CreateTable("idxcat", schema, tx)
		if err != nil {
			return nil, err
		}
	}
	layout, err := tblMgr.GetLayout("idxcat", tx)
	if err != nil {
		return nil, err
	}
	return &IndexMgr{layout, tblMgr, statMgr}, nil
}

func (im *IndexMgr) CreateIndex(idxname string, tblname string, fldname string, tx *tx.Transaction) error {
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return err
	}
	ts.Insert()
	ts.SetString("indexname", idxname)
	ts.SetString("tablename", tblname)
	ts.SetString("fieldname", fldname)
	ts.Close()
	return nil
}

func (im *IndexMgr) GetIndexInfo(tblname string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	result := make(map[string]*IndexInfo)
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return nil, err
	}
	next, err := ts.Next()
	if err != nil {
		return nil, err
	}
	for next {
		tblname, err := ts.GetString("tablename")
		if err != nil {
			return nil, err
		}
		if tblname == tblname {
			idxname, err := ts.GetString("indexname")
			if err != nil {
				return nil, err
			}
			fldname, err := ts.GetString("fieldname")
			if err != nil {
				return nil, err
			}
			tblLayout, err := im.tblMgr.GetLayout(tblname, tx)
			if err != nil {
				return nil, err
			}
			tblsi, err := im.statMgr.GetStatInfo(tblname, tblLayout, tx)
			if err != nil {
				return nil, err
			}
			ii := NewIndexInfo(idxname, fldname, tblLayout.Schema(), tx, tblsi)
			result[fldname] = ii
		}
		next, err = ts.Next()
		if err != nil {
			return nil, err
		}
	}
	ts.Close()
	return result, nil
}
