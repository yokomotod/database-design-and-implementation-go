package plan

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
)

var _ Plan = (*SelectPlan)(nil)

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

func (p *SelectPlan) BlocksAccessed() int32 {
	return p.plan.BlocksAccessed()
}

func (p *SelectPlan) RecordsOutput() int32 {
	return p.plan.RecordsOutput() / p.predicate.ReductionFactor(p.plan)
}

func (p *SelectPlan) DistinctValues(fieldName string) int32 {
	if p.predicate.EquatesWithConstant(fieldName) != nil {
		return 1
	}

	otherField := p.predicate.EquatesWithField(fieldName)
	if otherField != "" {
		return min(p.plan.DistinctValues(fieldName), p.plan.DistinctValues(otherField))
	}

	return p.plan.DistinctValues(fieldName)
}

func (p *SelectPlan) Schema() *record.Schema {
	return p.plan.Schema()
}

func (p *SelectPlan) Tree() *PlanNode {
	return NewPlanNode(fmt.Sprintf("Select(%s)", p.predicate),
		p,
		[]*PlanNode{p.plan.Tree()},
	)
}
