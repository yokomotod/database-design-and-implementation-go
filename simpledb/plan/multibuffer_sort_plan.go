package plan

import (
	"errors"
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"slices"
)

var _ Plan = (*SortPlan)(nil)

type MultibufferSortPlan struct {
	plan   Plan
	tx     *tx.Transaction
	schema *record.Schema
	comp   *query.RecordComparator
}

func NewMultibufferSortPlan(tx *tx.Transaction, plan Plan, sortFields []string) (*SortPlan, error) {
	return &SortPlan{
		plan:   plan,
		tx:     tx,
		schema: plan.Schema(),
		comp:   query.NewRecordComparator(sortFields),
	}, nil
}

func (sp *MultibufferSortPlan) Open() (query.Scan, error) {
	src, err := sp.plan.Open()
	if err != nil {
		return nil, fmt.Errorf("sp.plan.Open: %w", err)
	}
	runs, err := sp.multibufferSplitIntoRuns(src)
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

func (sp *MultibufferSortPlan) BlocksAccessed() int32 {
	mp := NewMaterializePlan(sp.tx, sp.plan)
	return mp.BlocksAccessed()
}

func (sp *MultibufferSortPlan) RecordsOutput() int32 {
	return sp.plan.RecordsOutput()
}

func (sp *MultibufferSortPlan) DistinctValues(fieldName string) int32 {
	return sp.plan.DistinctValues(fieldName)
}

func (sp *MultibufferSortPlan) Schema() *record.Schema {
	return sp.schema
}

func (sp *MultibufferSortPlan) multibufferSplitIntoRuns(src query.Scan) ([]*query.TempTable, error) {
	size := sp.BlocksAccessed()
	available := sp.tx.AvailableBuffers()
	numBuffs := query.BufferNeedsBestRoot(available, size)

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

	usedBuffs := int32(1)
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

		if usedBuffs < numBuffs {
			usedBuffs++
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

func (sp *MultibufferSortPlan) sortIntoTemp(valsList []map[string]*query.Constant) (*query.TempTable, error) {
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

func (sp *MultibufferSortPlan) sortInMemory(valsList []map[string]*query.Constant) {
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
func (sp *MultibufferSortPlan) doAMergeIteration(runs []*query.TempTable) ([]*query.TempTable, error) {
	available := sp.tx.AvailableBuffers()
	numBuffs := query.BufferNeedsBestRoot(available, int32(len(runs)))

	result := make([]*query.TempTable, 0)
	for len(runs) > 1 {
		runsToMerge := runs[:numBuffs]
		runs = runs[numBuffs:]
		merged, err := sp.mergeSeveralRuns(runsToMerge)
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

func (sp *MultibufferSortPlan) mergeSeveralRuns(runs []*query.TempTable) (*query.TempTable, error) {
	srcs := make([]query.Scan, 0, len(runs))
	for _, run := range runs {
		src, err := run.Open()
		if err != nil {
			return nil, fmt.Errorf("run.Open(): %w", err)
		}
		defer src.Close()
		srcs = append(srcs, src)
	}

	result := query.NewTempTable(sp.tx, sp.schema)
	dest, err := result.Open()
	if err != nil {
		return nil, fmt.Errorf("result.Open(): %w", err)
	}
	defer dest.Close()

	for {
		hasMores := make([]query.Scan, 0, len(srcs))
		for i := range srcs {
			hasMore, err := srcs[i].Next()
			if err != nil {
				return nil, fmt.Errorf("srcs[%d].Next(): %w", i, err)
			}

			if hasMore {
				hasMores = append(hasMores, srcs[i])
			}
		}

		if len(hasMores) == 0 {
			break
		}

		errs := make([]error, 0)
		slices.SortStableFunc(hasMores, func(a, b query.Scan) int {
			cmp, err := sp.comp.Compare(a, b)
			if err != nil {
				errs = append(errs, err)
			}

			return cmp
		})
		if len(errs) > 0 {
			return nil, fmt.Errorf("sp.comp.Compare: %w", errors.Join(errs...))
		}

		for _, src := range hasMores {
			err = sp.copy(src, dest)
			if err != nil {
				return nil, fmt.Errorf("sp.copy: %w", err)
			}
		}
	}

	return result, nil
}

func (sp *MultibufferSortPlan) copy(src query.Scan, dest query.UpdateScan) error {
	err := dest.Insert()
	if err != nil {
		return err
	}

	for _, fieldName := range sp.schema.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return fmt.Errorf("src.GetVal(%s): %w", fieldName, err)
		}
		err = dest.SetVal(fieldName, val)
		if err != nil {
			return fmt.Errorf("dest.SetVal(%s, %v): %w", fieldName, val, err)
		}
	}

	return nil
}

func (sp *MultibufferSortPlan) copyDummyAndReturnVals(src query.Scan, dest query.UpdateScan) (bool, map[string]*query.Constant, error) {
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

func (sp *MultibufferSortPlan) copyAllVals(valsList []map[string]*query.Constant, dest query.UpdateScan) error {
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
