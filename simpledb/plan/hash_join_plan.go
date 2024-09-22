package plan

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
)

var _ Plan = (*HashJoinPlan)(nil)

type HashJoinPlan struct {
	logger *logger.Logger

	p1, p2             Plan
	tx                 *tx.Transaction
	fldName1, fldName2 string
	schema             *record.Schema
}

func NewHashJoinPlan(tx *tx.Transaction, p1, p2 Plan, fldName1, fldName2 string) (*HashJoinPlan, error) {
	logger := logger.New("plan.HashJoinPlan", logger.Trace)
	if p1.BlocksAccessed() < p2.BlocksAccessed() {
		logger.Tracef("NewHashJoinPlan(): swap p1 and p2")
		p2, p1 = p1, p2
		fldName2, fldName1 = fldName1, fldName2
	}

	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())

	return &HashJoinPlan{
		logger: logger,

		p1:       p1,
		p2:       p2,
		tx:       tx,
		fldName1: fldName1,
		fldName2: fldName2,
		schema:   schema,
	}, nil
}

func (hjp *HashJoinPlan) Open() (query.Scan, error) {
	available := hjp.tx.AvailableBuffers()
	numBuffs := query.BufferNeedsBestFactor(available, hjp.p2.BlocksAccessed())

	t1, err := hjp.copyToTemp(hjp.p1)
	if err != nil {
		return nil, fmt.Errorf("copyToTemp1: %w", err)
	}

	t2, err := hjp.copyToTemp(hjp.p2)
	if err != nil {
		return nil, fmt.Errorf("copyToTemp2: %w", err)
	}

	buckets1, buckets2, err := hjp.recursiveSplitIntoBucket(t1, t2, numBuffs, numBuffs)
	if err != nil {
		return nil, fmt.Errorf("recursiveSplitIntoBucket: %w", err)
	}

	hjp.logger.Tracef("Open(): splitted into %d buckets", len(buckets1))
	return query.NewSelectScan(
		query.NewHashJoinScan(hjp.tx, buckets1, buckets2),
		query.NewPredicateWithTerm(
			query.NewTerm(
				query.NewExpressionWithField(hjp.fldName1),
				query.NewExpressionWithField(hjp.fldName2),
			),
		),
	), nil
}

func (hjp *HashJoinPlan) BlocksAccessed() int32 {
	return -1 // TODO
}

func (hjp *HashJoinPlan) RecordsOutput() int32 {
	return -1 // TODO
}

func (hjp *HashJoinPlan) DistinctValues(fieldName string) int32 {
	return -1 // TODO
}

func (hjp *HashJoinPlan) Schema() *record.Schema {
	return hjp.schema
}

func (hjp *HashJoinPlan) Tree() *PlanNode {
	return NewPlanNode("HashJoin", hjp, []*PlanNode{hjp.p1.Tree(), hjp.p2.Tree()})
}

func (hjp *HashJoinPlan) recursiveSplitIntoBucket(p1, p2 *query.TempTable, numBuffs, mod int32) ([]*query.TempTable, []*query.TempTable, error) {
	hjp.logger.Tracef("recursiveSplitIntoBucket(): p2.TotalBlkNum=%d, numBuffs=%d, mod=%d", p2.TotalBlkNum, numBuffs, mod)

	if p2.TotalBlkNum <= numBuffs {
		// fits in buffers
		return []*query.TempTable{p1}, []*query.TempTable{p2}, nil
	}

	hjp.logger.Tracef("recursiveSplitIntoBucket(): splitIntoBucket: lhs: %s", hjp.fldName1)
	buckets1, err := hjp.splitIntoBucket(p1, numBuffs, mod, hjp.fldName1)
	if err != nil {
		return nil, nil, fmt.Errorf("splitIntoBucket1: %w", err)
	}
	hjp.logger.Tracef("recursiveSplitIntoBucket(): splitIntoBucket: rhs: %s", hjp.fldName2)
	buckets2, err := hjp.splitIntoBucket(p2, numBuffs, mod, hjp.fldName2)
	if err != nil {
		return nil, nil, fmt.Errorf("splitIntoBucket2: %w", err)
	}

	if len(buckets2) == 1 {
		// not fit in buffers, but cannot be split anymore
		return buckets1, buckets2, nil
	}

	subBuckets1 := make([]*query.TempTable, 0)
	subBuckets2 := make([]*query.TempTable, 0)
	for i := range numBuffs {
		b1, b2, err := hjp.recursiveSplitIntoBucket(buckets1[i], buckets2[i], numBuffs, mod*numBuffs)
		if err != nil {
			return nil, nil, fmt.Errorf("recursiveSplitIntoBucket: %w", err)
		}

		subBuckets1 = append(subBuckets1, b1...)
		subBuckets2 = append(subBuckets2, b2...)
	}

	return subBuckets1, subBuckets2, nil
}

func (hjp *HashJoinPlan) splitIntoBucket(p *query.TempTable, numBuffs, mod int32, fldName string) ([]*query.TempTable, error) {
	buckets := make([]*query.TempTable, numBuffs)
	scans := make([]*query.TableScan, numBuffs)
	for i := range buckets {
		temp := query.NewTempTable(hjp.tx, p.Layout().Schema())
		scan, err := temp.Open()
		if err != nil {
			return nil, fmt.Errorf("temp.Open: %w", err)
		}
		scans[i] = scan
		buckets[i] = temp
	}

	scan, err := p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open: %w", err)
	}

	for {
		next, err := scan.Next()
		if err != nil {
			return nil, fmt.Errorf("scan1.Next: %w", err)
		}
		if !next {
			break
		}

		val, err := scan.GetVal(fldName)
		if err != nil {
			return nil, fmt.Errorf("scan1.GetVal: %w", err)
		}

		hash := val.HashCode()
		bucket := hash % mod

		err = scans[bucket].Insert()
		if err != nil {
			return nil, fmt.Errorf("buckets1[bucket].Insert: %w", err)
		}

		for _, fldname := range p.Layout().Schema().Fields() {
			val, err := scan.GetVal(fldname)
			if err != nil {
				return nil, fmt.Errorf("scan1.GetVal: %w", err)
			}
			err = scans[bucket].SetVal(fldname, val)
			if err != nil {
				return nil, fmt.Errorf("buckets1[bucket].SetVal: %w", err)
			}
		}
		buckets[bucket].TotalBlkNum = scans[bucket].TotalBlkNum
	}
	for _, scan := range scans {
		scan.Close()
	}

	return buckets, nil
}

func (hjp *HashJoinPlan) copyToTemp(p Plan) (*query.TempTable, error) {
	src, err := p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open: %w", err)
	}
	defer src.Close()

	sch := p.Schema()

	t := query.NewTempTable(hjp.tx, sch)
	dest, err := t.Open()
	if err != nil {
		return nil, fmt.Errorf("t.Open: %w", err)
	}
	defer dest.Close()

	for {
		next, err := src.Next()
		if err != nil {
			return nil, fmt.Errorf("src.Next: %w", err)
		}

		if !next {
			break
		}

		err = dest.Insert()
		if err != nil {
			return nil, fmt.Errorf("dest.Insert: %w", err)
		}

		for _, fldname := range sch.Fields() {
			val, err := src.GetVal(fldname)
			if err != nil {
				return nil, fmt.Errorf("src.GetVal: %w", err)
			}

			err = dest.SetVal(fldname, val)
			if err != nil {
				return nil, fmt.Errorf("dest.SetVal: %w", err)
			}
		}
	}

	t.TotalBlkNum = dest.TotalBlkNum

	return t, nil
}
