package plan

import (
	"errors"
	"simpledb/metadata"
	"simpledb/query"
	"simpledb/record"
)

type IndexJoinPlan struct {
	plan1     Plan
	plan2     Plan
	indexInfo *metadata.IndexInfo
	joinField string
	schema    *record.Schema
}

func NewIndexJoinPlan(plan1 Plan, plan2 Plan, indexInfo *metadata.IndexInfo, joinField string) *IndexJoinPlan {
	sch := record.NewSchema()
	sch.AddAll(plan1.Schema())
	sch.AddAll(plan2.Schema())
	return &IndexJoinPlan{plan1, plan2, indexInfo, joinField, sch}
}

func (p *IndexJoinPlan) Open() (query.Scan, error) {
	scan, err := p.plan1.Open()
	if err != nil {
		return nil, err
	}
	ts, ok := scan.(*query.TableScan)
	if !ok {
		return nil, errors.New("Open: plan is not a table plan")
	}
	idx, err := p.indexInfo.Open()
	if err != nil {
		return nil, err
	}
	return query.NewIndexJoinScan(scan, idx, p.joinField, ts)
}

func (p *IndexJoinPlan) BlocksAccessed() int {
	return p.plan1.BlocksAccessed() + (p.plan1.RecordsOutput() * p.indexInfo.BlocksAccessed()) + p.RecordsOutput()
}

func (p *IndexJoinPlan) RecordsOutput() int {
	return p.plan1.RecordsOutput() * p.indexInfo.RecordsOutput()
}

func (p *IndexJoinPlan) DistinctValues(fieldName string) int {
	if p.plan1.Schema().HasField(fieldName) {
		return p.plan1.DistinctValues(fieldName)
	}
	return p.plan2.DistinctValues(fieldName)
}

func (p *IndexJoinPlan) Schema() *record.Schema {
	return p.schema
}
