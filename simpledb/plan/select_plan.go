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
	return p.plan.RecordsOutput() / p.predicate.ReductionFactor(p)
}

func (p *SelectPlan) DistinctValues(fieldName string) int {
	if p.predicate.EquatesWithConstant(fieldName) != nil {
		return 1
	} else {
		otherField := p.predicate.EquatesWithField(fieldName)
		if otherField != "" {
			return min(p.plan.DistinctValues(fieldName), p.plan.DistinctValues(otherField))
		} else {
			return p.plan.DistinctValues(fieldName)
		}
	}
}

func (p *SelectPlan) Schema() *record.Schema {
	return p.plan.Schema()
}
