package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

type Manager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewManager(isNew bool, tx *tx.Transaction) (*Manager, error) {
	tableManager, err := NewTableManager(isNew, tx)
	if err != nil {
		return nil, err
	}
	viewManager, err := NewViewManager(isNew, tableManager, tx)
	if err != nil {
		return nil, err
	}
	statManager, err := NewStatManager(tableManager, tx)
	if err != nil {
		return nil, err
	}
	indexManager, err := NewIndexManager(isNew, tableManager, statManager, tx)
	if err != nil {
		return nil, err
	}
	return &Manager{tableManager, viewManager, statManager, indexManager}, nil
}

func (mm *Manager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	return mm.tableManager.CreateTable(tableName, schema, tx)
}

func (mm *Manager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	return mm.tableManager.GetLayout(tableName, tx)
}

func (mm *Manager) CreateView(viewName string, viewDef string, tx *tx.Transaction) error {
	return mm.viewManager.CreateView(viewName, viewDef, tx)
}

func (mm *Manager) GetViewDef(viewName string, tx *tx.Transaction) (string, error) {
	return mm.viewManager.GetViewDef(viewName, tx)
}

func (mm *Manager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	return mm.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mm *Manager) GetIndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	return mm.indexManager.GetIndexInfo(tableName, tx)
}

func (mm *Manager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	return mm.statManager.GetStatInfo(tableName, layout, tx)
}
