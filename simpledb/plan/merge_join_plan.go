package plan

import (
	"errors"
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

var _ Plan = (*MergeJoinPlan)(nil)

type MergeJoinPlan struct {
	p1, p2             *SortPlan
	fldName1, fldName2 string
	sch                *record.Schema
}

func NewMergeJoinPlan(tx *tx.Transaction, p1, p2 Plan, fldName1, fldName2 string) (*MergeJoinPlan, error) {
	sortPlan1, err := NewSortPlan(tx, p1, []string{fldName1})
	if err != nil {
		return nil, fmt.Errorf("NewSortPlan for p1: %w", err)
	}
	sortPlan2, err := NewSortPlan(tx, p2, []string{fldName2})
	if err != nil {
		return nil, fmt.Errorf("NewSortPlan for p2: %w", err)
	}
	sch := record.NewSchema()
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())

	return &MergeJoinPlan{
		p1:       sortPlan1,
		p2:       sortPlan2,
		fldName1: fldName1,
		fldName2: fldName2,
		sch:      sch,
	}, nil
}

func (mjp *MergeJoinPlan) Open() (query.Scan, error) {
	s1, err := mjp.p1.Open()
	if err != nil {
		return nil, fmt.Errorf("mjp.p1.Open: %w", err)
	}
	ss1, ok := s1.(*query.SortScan)
	if !ok {
		return nil, errors.New("s1 is not a SortScan")
	}
	s2, err := mjp.p2.Open()
	if err != nil {
		return nil, fmt.Errorf("mjp.p2.Open: %w", err)
	}
	ss2, ok := s2.(*query.SortScan)
	if !ok {
		return nil, errors.New("s2 is not a SortScan")
	}
	return query.NewMergeJoinScan(ss1, ss2, mjp.fldName1, mjp.fldName2), nil
}

func (mjp *MergeJoinPlan) BlocksAccessed() int {
	return mjp.p1.BlocksAccessed() + mjp.p2.BlocksAccessed()
}

func (mjp *MergeJoinPlan) RecordsOutput() int {
	maxVals := max(mjp.p1.DistinctValues(mjp.fldName1),
		mjp.p2.DistinctValues(mjp.fldName2))
	return mjp.p1.RecordsOutput() * mjp.p2.RecordsOutput() / maxVals
}

func (mjp *MergeJoinPlan) DistinctValues(fieldName string) int {
	if mjp.sch.HasField(fieldName) {
		return mjp.p1.DistinctValues(fieldName)
	}
	return mjp.p2.DistinctValues(fieldName)
}

func (mjp *MergeJoinPlan) Schema() *record.Schema {
	return mjp.sch
}
