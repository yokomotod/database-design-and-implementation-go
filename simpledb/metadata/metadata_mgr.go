package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

type MetadataMgr struct {
	tblMgr  *TableMgr
	viewMgr *ViewMgr
	statMgr *StatMgr
	idxMgr  *IndexMgr
}

func NewMetadataMgr(isNew bool, tx *tx.Transaction) (*MetadataMgr, error) {
	tblmgr := NewTableMgr(isNew, tx)
	viewmgr, err := NewViewMgr(isNew, tblmgr, tx)
	if err != nil {
		return nil, err
	}
	statmgr, err := NewStatMgr(tblmgr, tx)
	if err != nil {
		return nil, err
	}
	idxmgr, err := NewIndexMgr(isNew, tblmgr, statmgr, tx)
	if err != nil {
		return nil, err
	}
	return &MetadataMgr{tblmgr, viewmgr, statmgr, idxmgr}, nil
}

func (mm *MetadataMgr) CreateTable(tblname string, schema *record.Schema, tx *tx.Transaction) error {
	return mm.tblMgr.CreateTable(tblname, schema, tx)
}

func (mm *MetadataMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	return mm.tblMgr.GetLayout(tblname, tx)
}

func (mm *MetadataMgr) CreateView(viewname string, viewdef string, tx *tx.Transaction) error {
	return mm.viewMgr.CreateView(viewname, viewdef, tx)
}

func (mm *MetadataMgr) GetViewDef(viewname string, tx *tx.Transaction) (string, error) {
	return mm.viewMgr.GetViewDef(viewname, tx)
}

func (mm *MetadataMgr) CreateIndex(idxname string, tblname string, fldname string, tx *tx.Transaction) error {
	return mm.idxMgr.CreateIndex(idxname, tblname, fldname, tx)
}

func (mm *MetadataMgr) GetIndexInfo(tblname string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	return mm.idxMgr.GetIndexInfo(tblname, tx)
}

func (mm *MetadataMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	return mm.statMgr.GetStatInfo(tblname, layout, tx)
}
