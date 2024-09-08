package query

import (
	"fmt"
	"simpledb/record"
	"simpledb/tx"
	"sync"
)

var nextTableNum = 0
var mux = &sync.Mutex{}

type TempTable struct {
	tx        *tx.Transaction
	TableName string
	layout    *record.Layout
}

func NewTempTable(tx *tx.Transaction, sch *record.Schema) *TempTable {
	layout := record.NewLayoutFromSchema(sch)

	return &TempTable{
		tx:        tx,
		TableName: newTableName(),
		layout:    layout,
	}
}

func (tt *TempTable) Open() (*TableScan, error) {
	scan, err := NewTableScan(tt.tx, tt.TableName, tt.layout)
	if err != nil {
		return nil, fmt.Errorf("tt.Open: %w", err)
	}
	return scan, nil
}

func (tt *TempTable) Layout() *record.Layout {
	return tt.layout
}

func newTableName() string {
	mux.Lock()
	defer mux.Unlock()

	nextTableNum++
	return fmt.Sprintf("temp%d", nextTableNum)
}
