package plan

import (
	"simpledb/query"
	"simpledb/record"
)

type SelectPlan struct {
	plan      Plan
	predicate *query.Predicate
}

func NewSelectPlan(p Plan, pred *query.Predicate) (*SelectPlan, error) {
	return &SelectPlan{p, pred}, nil
}

func (p *SelectPlan) Open() (query.Scan, error) {
	scan, error := p.plan.Open()
	if error != nil {
		return nil, error
	}
	return query.NewSelectScan(scan, p.predicate), nil
}

func (p *SelectPlan) BlocksAccessed() int {
	return p.plan.BlocksAccessed()
}

func (p *SelectPlan) RecordsOutput() int {
	// TODO implement
	return 0
}

func (p *SelectPlan) DistinctValues(fieldName string) int {
	// TODO implement
	return 0
}

func (p *SelectPlan) Schema() *record.Schema {
	return p.plan.Schema()
}
