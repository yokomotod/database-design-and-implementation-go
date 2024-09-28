package plan

import (
	"fmt"
	"simpledb/metadata"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
)

var _ Plan = (*TablePlan)(nil)

type TablePlan struct {
	tableName string
	tx        *tx.Transaction
	layout    *record.Layout
	statInfo  *metadata.StatInfo
}

func NewTablePlan(tx *tx.Transaction, tableName string, md *metadata.Manager) (*TablePlan, error) {
	logger := logger.New("plan.TablePlan", logger.Info)
	logger.Tracef("(%q) NewTablePlan", tableName)

	layout, err := md.GetLayout(tableName, tx)
	if err != nil {
		return nil, err
	}
	statInfo, err := md.GetStatInfo(tableName, layout, tx)
	if err != nil {
		return nil, err
	}
	return &TablePlan{tableName, tx, layout, statInfo}, nil
}

func (p *TablePlan) Open() (query.Scan, error) {
	return query.NewTableScan(p.tx, p.tableName, p.layout)
}

func (p *TablePlan) BlocksAccessed() int32 {
	return p.statInfo.BlocksAccessed()
}

func (p *TablePlan) RecordsOutput() int32 {
	return p.statInfo.RecordsOutput()
}

func (p *TablePlan) DistinctValues(fieldName string) int32 {
	return p.statInfo.DistinctValues(fieldName)
}

func (p *TablePlan) Schema() *record.Schema {
	return p.layout.Schema()
}

func (p *TablePlan) Tree() *PlanNode {
	return NewPlanNode(fmt.Sprintf("Table(%s)", p.tableName), p, nil)
}
