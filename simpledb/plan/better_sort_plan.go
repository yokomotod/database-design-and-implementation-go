package plan

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"slices"
)

var _ Plan = (*SortPlan)(nil)

type BetterSortPlan struct {
	plan   Plan
	tx     *tx.Transaction
	schema *record.Schema
	comp   *query.RecordComparator
}

func NewBetterSortPlan(tx *tx.Transaction, plan Plan, sortFields []string) (*SortPlan, error) {
	return &SortPlan{
		plan:   plan,
		tx:     tx,
		schema: plan.Schema(),
		comp:   query.NewRecordComparator(sortFields),
	}, nil
}

func (sp *BetterSortPlan) Open() (query.Scan, error) {
	src, err := sp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("sp.plan.Open: %w", err)
	}
	runs, err := sp.onebufferSplitIntoRuns(src)
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

func (sp *BetterSortPlan) BlocksAccessed() int32 {
	mp := NewMaterializePlan(sp.tx, sp.plan)
	return mp.BlocksAccessed()
}

func (sp *BetterSortPlan) RecordsOutput() int32 {
	return sp.plan.RecordsOutput()
}

func (sp *BetterSortPlan) DistinctValues(fieldName string) int32 {
	return sp.plan.DistinctValues(fieldName)
}

func (sp *BetterSortPlan) Schema() *record.Schema {
	return sp.schema
}

func (sp *BetterSortPlan) onebufferSplitIntoRuns(src query.Scan) ([]*query.TempTable, error) {
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
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	valsList := make([]map[string]*query.Constant, 0)

	for {
		next, vals, err := sp.copyDummyAndReturnVals(src, currentScan)
		if err != nil {
			return nil, fmt.Errorf("sp.copy: %w", err)
		}
		valsList = append(valsList, vals)

		if !next {
			break
		}

		atLastBlock, err := currentScan.AtLastBlock()
		if err != nil {
			return nil, fmt.Errorf("currentScan.AtLastBlock: %w", err)
		}

		if !atLastBlock {
			continue
		}

		currentScan.Close()

		sortedTemp, err := sp.sortIntoTemp(valsList)
		if err != nil {
			return nil, err
		}
		temps = append(temps, sortedTemp)

		valsList = make([]map[string]*query.Constant, 0)
		currentTemp = query.NewTempTable(sp.tx, sp.schema)
		temps = append(temps, currentTemp)
		currentScan, err = currentTemp.Open()
		if err != nil {
			return nil, err
		}
	}

	currentScan.Close()
	sortedTemp, err := sp.sortIntoTemp(valsList)
	if err != nil {
		return nil, err
	}
	temps = append(temps, sortedTemp)

	return temps, nil
}

func (sp *BetterSortPlan) sortIntoTemp(valsList []map[string]*query.Constant) (*query.TempTable, error) {
	sp.sortInMemory(valsList)

	sortedTemp := query.NewTempTable(sp.tx, sp.schema)
	sortedScan, err := sortedTemp.Open()
	if err != nil {
		return nil, err
	}
	defer sortedScan.Close()

	err = sp.copyAllVals(valsList, sortedScan)
	if err != nil {
		return nil, err
	}

	return sortedTemp, nil
}

func (sp *BetterSortPlan) sortInMemory(valsList []map[string]*query.Constant) {
	slices.SortStableFunc(valsList, func(a, b map[string]*query.Constant) int {
		cmp, err := sp.comp.CompareMap(a, b)
		if err != nil {
			panic(err)
		}
		return cmp
	})
}

// func (sBetterp *SortPlan) multibufferSplitIntoRuns(src query.Scan) ([]*query.TempTable, error) {
// 	available := sp.tx.AvailableBuffers()
// 	size := sp.BlocksAccessed()
// 	numBuffs := query.BufferNeedsBestRoot(available, size)

// }
func (sp *BetterSortPlan) doAMergeIteration(runs []*query.TempTable) ([]*query.TempTable, error) {
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

func (sp *BetterSortPlan) mergeTwoRuns(p1 *query.TempTable, p2 *query.TempTable) (*query.TempTable, error) {
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

func (sp *BetterSortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
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

func (sp *BetterSortPlan) copyDummyAndReturnVals(src query.Scan, dest query.UpdateScan) (bool, map[string]*query.Constant, error) {
	err := dest.Insert()
	if err != nil {
		return false, nil, err
	}

	values := make(map[string]*query.Constant, len(sp.schema.Fields()))
	for _, fieldName := range sp.schema.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, nil, fmt.Errorf("src.GetVal(%s): %w", fieldName, err)
		}

		// AtLastBlock() を使うだけなのでSetVal()はしない

		values[fieldName] = val
	}

	next, err := src.Next()
	if err != nil {
		return false, nil, err
	}

	return next, values, nil
}

func (sp *BetterSortPlan) copyAllVals(valsList []map[string]*query.Constant, dest query.UpdateScan) error {
	err := dest.Insert()
	if err != nil {
		return err
	}

	for _, vals := range valsList {
		for _, fieldName := range sp.schema.Fields() {
			val, ok := vals[fieldName]
			if !ok {
				return fmt.Errorf("vals has no key %s", fieldName)
			}

			err = dest.SetVal(fieldName, val)
			if err != nil {
				return fmt.Errorf("dest.SetVal(%s, %v): %w", fieldName, val, err)
			}
		}
	}

	return nil
}
