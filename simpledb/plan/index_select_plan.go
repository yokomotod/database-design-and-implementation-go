package plan

import (
	"errors"
	"simpledb/metadata"
	"simpledb/query"
	"simpledb/record"
)

var _ Plan = (*IndexSelectPlan)(nil)

type IndexSelectPlan struct {
	plan      Plan
	indexInfo *metadata.IndexInfo
	val       *query.Constant
}

func NewIndexSelectPlan(p Plan, indexInfo *metadata.IndexInfo, val *query.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{p, indexInfo, val}
}

func (p *IndexSelectPlan) Open() (query.Scan, error) {
	scan, err := p.plan.Open()
	if err != nil {
		return nil, err
	}
	tableScan, ok := scan.(*query.TableScan)
	if !ok {
		return nil, errors.New("Open: plan is not a table plan")
	}
	idx, err := p.indexInfo.Open()
	if err != nil {
		return nil, err
	}
	return query.NewIndexSelectScan(tableScan, idx, p.val), nil
}

func (p *IndexSelectPlan) BlocksAccessed() int32 {
	return p.indexInfo.BlocksAccessed() + p.RecordsOutput()
}

func (p *IndexSelectPlan) RecordsOutput() int32 {
	return p.indexInfo.RecordsOutput()
}

func (p *IndexSelectPlan) DistinctValues(fieldName string) int32 {
	return p.indexInfo.DistinctValues(fieldName)
}

func (p *IndexSelectPlan) Schema() *record.Schema {
	return p.plan.Schema()
}

func (p *IndexSelectPlan) Tree() *PlanNode {
	return NewPlanNode("IndexSelect", p, []*PlanNode{p.plan.Tree()})
}
