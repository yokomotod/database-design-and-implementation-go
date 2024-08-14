package plan

import (
	"simpledb/query"
	"simpledb/record"
)

type ProjectPlan struct {
	plan   Plan
	schema *record.Schema
}

func NewProjectPlan(p Plan, fieldList []string) (*ProjectPlan, error) {
	sch := record.NewSchema()
	for _, field := range fieldList {
		// TODO check if the field exists in the underlying Schema
		sch.Add(field, p.Schema())
	}
	return &ProjectPlan{p, sch}, nil
}

func (p *ProjectPlan) Open() (query.Scan, error) {
	scan, err := p.plan.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(scan, p.schema.Fields()), nil
}

func (p *ProjectPlan) BlocksAccessed() int {
	return p.plan.BlocksAccessed()
}

func (p *ProjectPlan) RecordsOutput() int {
	return p.plan.RecordsOutput()
}

func (p *ProjectPlan) DistinctValues(fieldName string) int {
	return p.plan.DistinctValues(fieldName)
}

func (p *ProjectPlan) Schema() *record.Schema {
	return p.schema
}
