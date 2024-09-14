package plan

import (
	"errors"
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

var _ Plan = (*GroupByPlan)(nil)

type GroupByPlan struct {
	plan        *SortPlan
	groupFields []string
	aggFns      []query.AggregationFn
	schema      *record.Schema
}

func NewGroupByPlan(tx *tx.Transaction, plan Plan, groupFields []string, aggFns []query.AggregationFn) (*GroupByPlan, error) {
	sortPlan, err := NewSortPlan(tx, plan, groupFields)
	if err != nil {
		return nil, fmt.Errorf("NewSortPlan: %w", err)
	}
	schema := record.NewSchema()
	for _, fldname := range groupFields {
		schema.Add(fldname, plan.Schema())
	}
	for _, fn := range aggFns {
		schema.AddIntField(fn.FieldName())
	}
	return &GroupByPlan{
		plan:        sortPlan,
		groupFields: groupFields,
		aggFns:      aggFns,
		schema:      schema,
	}, nil
}

func (gp *GroupByPlan) Open() (query.Scan, error) {
	s, err := gp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("gp.plan.Open: %w", err)
	}
	ss, ok := s.(*query.SortScan)
	if !ok {
		return nil, errors.New("s is not a SortScan")
	}
	return query.NewGroupByScan(ss, gp.groupFields, gp.aggFns), nil
}

func (gp *GroupByPlan) BlocksAccessed() int {
	return gp.plan.BlocksAccessed()
}

func (gp *GroupByPlan) RecordsOutput() int {
	numgroups := 1
	for _, fldname := range gp.groupFields {
		numgroups *= gp.plan.DistinctValues(fldname)
	}
	return numgroups
}

func (gp *GroupByPlan) DistinctValues(fieldName string) int {
	if gp.schema.HasField(fieldName) {
		return gp.plan.DistinctValues(fieldName)
	}
	return gp.RecordsOutput()
}

func (gp *GroupByPlan) Schema() *record.Schema {
	return gp.schema
}
