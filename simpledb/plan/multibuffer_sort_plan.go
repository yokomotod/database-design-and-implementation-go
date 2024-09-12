package plan

import (
	"errors"
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
	"slices"
)

var _ Plan = (*SortPlan)(nil)

type MultibufferSortPlan struct {
	logger *logger.Logger

	plan   Plan
	tx     *tx.Transaction
	schema *record.Schema
	comp   *query.RecordComparator
}

func NewMultibufferSortPlan(tx *tx.Transaction, plan Plan, sortFields []string) (*MultibufferSortPlan, error) {
	return &MultibufferSortPlan{
		logger: logger.New("plan.MultibufferSortPlan", logger.Trace),
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

	sc, err := query.NewSortScan(runs, sp.comp) // SortScanである必要ない？
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
	sp.logger.Tracef("multibufferSplitIntoRuns(): numBuffs: %d", numBuffs)

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
		if len(temps) >= 10 {
			panic("too many valsList")
		}
		sp.logger.Tracef("multibufferSplitIntoRuns(): copyDummyAndReturnVals")
		next, vals, err := sp.copyDummyAndReturnVals(src, currentScan)
		if err != nil {
			return nil, fmt.Errorf("sp.copy: %w", err)
		}
		valsList = append(valsList, vals)

		if !next {
			break
		}

		atLastBlock, err := currentScan.AtLastBlock()
		sp.logger.Tracef("multibufferSplitIntoRuns(): AtLastBlock: %t", atLastBlock)
		if err != nil {
			return nil, fmt.Errorf("currentScan.AtLastBlock: %w", err)
		}

		if !atLastBlock {
			continue
		}

		canInsert, err := currentScan.CanInsertCurrentBlock()
		if err != nil {
			return nil, fmt.Errorf("currentScan.CanInsertCurrentBlock: %w", err)
		}
		sp.logger.Tracef("multibufferSplitIntoRuns(): canInsert: %t, usedBuffs: %d", canInsert, usedBuffs)
		if canInsert {
			continue
		}

		usedBuffs++
		if usedBuffs < numBuffs {
			continue
		}

		currentScan.Close()

		sortedTemp, err := sp.sortIntoTemp(valsList)
		if err != nil {
			return nil, err
		}
		temps = append(temps, sortedTemp)

		valsList = make([]map[string]*query.Constant, 0)
		usedBuffs = 1
		currentTemp = query.NewTempTable(sp.tx, sp.schema)
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

	sp.logger.Tracef("multibufferSplitIntoRuns(): done: len(runs)=%v", len(temps))

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
	sp.logger.Tracef("sortInMemory(): before")
	for _, vals := range valsList {
		v := make(map[string]string, len(vals))
		for fieldName, val := range vals {
			v[fieldName] = val.String()
		}
		sp.logger.Tracef("%+v", v)
	}
	slices.SortStableFunc(valsList, func(a, b map[string]*query.Constant) int {
		cmp, err := sp.comp.CompareMap(a, b)
		if err != nil {
			panic(err)
		}
		return cmp
	})
	sp.logger.Tracef("sortInMemory(): after")
	for _, vals := range valsList {
		v := make(map[string]string, len(vals))
		for fieldName, val := range vals {
			v[fieldName] = val.String()
		}
		sp.logger.Tracef("%+v", v)
	}
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
		if numBuffs > int32(len(runs)) {
			numBuffs = int32(len(runs))
		}
		runsToMerge := runs[:numBuffs]
		runs = runs[numBuffs:]
		sp.logger.Tracef("doAMergeIteration(): mergeSeveralRuns: len(runsToMerge)=%d", len(runsToMerge))
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

	hasMores := make(map[query.Scan]struct{})

	for i := range srcs {
		hasMore, err := srcs[i].Next()
		if err != nil {
			return nil, fmt.Errorf("srcs[%d].Next(): %w", i, err)
		}

		if hasMore {
			sp.logger.Tracef("mergeSeveralRuns(): srcs[%d]: hasMore=true", i)
			eid, err := srcs[i].GetInt("eid")
			if err != nil {
				return nil, fmt.Errorf("srcs[%d].GetInt(eid): %w", i, err)
			}
			sp.logger.Tracef("mergeSeveralRuns(): srcs[%d]: eid=%d", i, eid)
			hasMores[srcs[i]] = struct{}{}
		}
	}

	for {
		if len(hasMores) == 0 {
			break
		}

		errs := make([]error, 0)
		srcs := make([]query.Scan, 0, len(hasMores))
		for src := range hasMores {
			srcs = append(srcs, src)
		}
		slices.SortStableFunc(srcs, func(a, b query.Scan) int {
			cmp, err := sp.comp.Compare(a, b)
			if err != nil {
				errs = append(errs, err)
			}

			return cmp
		})
		if len(errs) > 0 {
			return nil, fmt.Errorf("sp.comp.Compare: %w", errors.Join(errs...))
		}

		// for _, src := range hasMores {
		hasMore, err := sp.copy(srcs[0], dest)
		if err != nil {
			return nil, fmt.Errorf("sp.copy: %w", err)
		}

		if !hasMore {
			delete(hasMores, srcs[0])
		}
		// }
	}

	return result, nil
}

func (sp *MultibufferSortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
	err := dest.Insert()
	if err != nil {
		return false, err
	}

	for _, fieldName := range sp.schema.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, fmt.Errorf("src.GetVal(%s): %w", fieldName, err)
		}
		sp.logger.Tracef("copy(): fieldName=%s, val=%v", fieldName, val)
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
	for i, vals := range valsList {
		err := dest.Insert()
		if err != nil {
			return err
		}

		for _, fieldName := range sp.schema.Fields() {
			val, ok := vals[fieldName]
			if !ok {
				return fmt.Errorf("vals has no key %s", fieldName)
			}

			sp.logger.Tracef("copyAllVals(): i=%d, fieldName=%s, val=%v", i, fieldName, val)
			err = dest.SetVal(fieldName, val)
			if err != nil {
				return fmt.Errorf("dest.SetVal(%s, %v): %w", fieldName, val, err)
			}
		}
	}

	return nil
}
