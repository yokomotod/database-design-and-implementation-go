package plan

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

var _ Plan = (*SortPlan)(nil)

type SortPlan struct {
	plan   Plan
	tx     *tx.Transaction
	schema *record.Schema
	comp   *query.RecordComparator
}

func NewSortPlan(tx *tx.Transaction, plan Plan, sortFields []string) (*SortPlan, error) {
	return &SortPlan{
		plan:   plan,
		tx:     tx,
		schema: plan.Schema(),
		comp:   query.NewRecordComparator(sortFields),
	}, nil
}

func (sp *SortPlan) Open() (query.Scan, error) {
	src, err := sp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("sp.plan.Open: %w", err)
	}
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, fmt.Errorf("sp.splitIntoRuns: %w", err)
	}
	src.Close()
	for len(runs) > 2 {
		runs, err = sp.doAMergeIteration(runs)
		if err != nil {
			return nil, fmt.Errorf("sp.doAMergeIteration: %w", err)
		}
	}

	sc, err := query.NewSortScan(runs, sp.comp)
	if err != nil {
		return nil, fmt.Errorf("query.NewSortScan: %w", err)
	}
	return sc, nil
}

func (sp *SortPlan) BlocksAccessed() int {
	mp := NewMaterializePlan(sp.tx, sp.plan)
	return mp.BlocksAccessed()
}

func (sp *SortPlan) RecordsOutput() int {
	return sp.plan.RecordsOutput()
}

func (sp *SortPlan) DistinctValues(fieldName string) int {
	return sp.plan.DistinctValues(fieldName)
}

func (sp *SortPlan) Schema() *record.Schema {
	return sp.schema
}

func (sp *SortPlan) splitIntoRuns(src query.Scan) ([]*query.TempTable, error) {
	temps := make([]*query.TempTable, 0)
	err := src.BeforeFirst()
	if err != nil {
		return nil, fmt.Errorf("src.BeforeFirst: %w", err)
	}

	next, err := src.Next()
	if err != nil {
		return nil, err
	}
	if !next {
		return temps, nil
	}

	currentTemp := query.NewTempTable(sp.tx, sp.schema)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	// src < currentScan になるまでコピーし続けることで、 昇順にソートされた TempTable（run）が作成される。
	// 順序が崩れたら新しい TempTable にコピーしていく。
	// ex. [2, 6, 20, 4, 1, 16, 19, 3, 18] => [2, 6, 10], [4], [1, 16, 19], [3, 18]
	for {
		next, err = sp.copy(src, currentScan)
		if err != nil {
			return nil, fmt.Errorf("sp.copy: %w", err)
		}
		if !next {
			break
		}

		cmp, err := sp.comp.Compare(src, currentScan)
		if err != nil {
			return nil, fmt.Errorf("sp.comp.Compare: %w", err)
		}
		if cmp < 0 { // src < currentScan
			currentScan.Close()
			currentTemp = query.NewTempTable(sp.tx, sp.schema)
			temps = append(temps, currentTemp)
			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
			}
		}
	}

	currentScan.Close()
	return temps, nil
}

func (sp *SortPlan) doAMergeIteration(runs []*query.TempTable) ([]*query.TempTable, error) {
	result := make([]*query.TempTable, 0)
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]
		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil, fmt.Errorf("sp.mergeTwoRuns: %w", err)
		}
		result = append(result, merged)
	}

	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result, nil
}

func (sp *SortPlan) mergeTwoRuns(p1 *query.TempTable, p2 *query.TempTable) (*query.TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, fmt.Errorf("p1.Open(): %w", err)
	}
	defer src1.Close()

	src2, err := p2.Open()
	if err != nil {
		return nil, fmt.Errorf("p2.Open(): %w", err)
	}
	defer src2.Close()

	result := query.NewTempTable(sp.tx, sp.schema)
	dest, err := result.Open()
	if err != nil {
		return nil, fmt.Errorf("result.Open(): %w", err)
	}
	defer dest.Close()

	hasMore1, err := src1.Next()
	if err != nil {
		return nil, fmt.Errorf("src1.Next(): %w", err)
	}
	hasMore2, err := src2.Next()
	if err != nil {
		return nil, fmt.Errorf("src2.Next(): %w", err)
	}

	for hasMore1 && hasMore2 {
		cmp, err := sp.comp.Compare(src1, src2)
		if err != nil {
			return nil, fmt.Errorf("sp.compare: %w", err)
		}
		if cmp < 0 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, fmt.Errorf("copy src1: %w", err)
			}
		} else {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, fmt.Errorf("copy src2: %w", err)
			}
		}
	}
	if hasMore1 {
		for hasMore1 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, fmt.Errorf("copy src1: %w", err)
			}
		}
	} else {
		for hasMore2 {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, fmt.Errorf("copy src2: %w", err)
			}
		}
	}

	return result, nil
}

func (sp *SortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
	err := dest.Insert()
	if err != nil {
		return false, err
	}

	for _, fieldName := range sp.schema.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, fmt.Errorf("src.GetVal(%s): %w", fieldName, err)
		}
		err = dest.SetVal(fieldName, val)
		if err != nil {
			return false, fmt.Errorf("dest.SetVal(%s, %v): %w", fieldName, val, err)
		}
	}

	next, err := src.Next()
	if err != nil {
		return false, err
	}

	return next, nil
}
