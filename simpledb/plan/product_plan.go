package plan

import (
	"simpledb/query"
	"simpledb/record"
)

var _ Plan = (*ProductPlan)(nil)

type ProductPlan struct {
	p1, p2 Plan
	schema *record.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) (*ProductPlan, error) {
	sch := record.NewSchema()
	// TODO check ambiguous field names
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())
	return &ProductPlan{p1, p2, sch}, nil
}

func (p *ProductPlan) Open() (query.Scan, error) {
	s1, err := p.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := p.p2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2)
}

func (p *ProductPlan) BlocksAccessed() int32 {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.p2.BlocksAccessed())
}

func (p *ProductPlan) RecordsOutput() int32 {
	// fmt.Printf("ProductPlan.RecordsOutput: %d * %d = %d\n", p.p1.RecordsOutput(), p.p2.RecordsOutput(), p.p1.RecordsOutput()*p.p2.RecordsOutput())
	return p.p1.RecordsOutput() * p.p2.RecordsOutput()
}

func (p *ProductPlan) DistinctValues(fieldName string) int32 {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	} else {
		return p.p2.DistinctValues(fieldName)
	}
}

func (p *ProductPlan) Schema() *record.Schema {
	return p.schema
}
