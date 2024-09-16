package plan

import (
	"fmt"
	"math"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
)

var _ Plan = (*MaterializePlan)(nil)

type MaterializePlan struct {
	srcPlan Plan
	tx      *tx.Transaction
}

func NewMaterializePlan(tx *tx.Transaction, srcPlan Plan) *MaterializePlan {
	return &MaterializePlan{
		srcPlan: srcPlan,
		tx:      tx,
	}
}

func (p *MaterializePlan) Open() (query.Scan, error) {
	sch := p.srcPlan.Schema()
	temp := query.NewTempTable(p.tx, sch)
	src, err := p.srcPlan.Open()
	if err != nil {
		return nil, fmt.Errorf("p.srcPlan.Open(): %w", err)
	}
	defer src.Close()

	dest, err := temp.Open()
	if err != nil {
		return nil, fmt.Errorf("temp.Open(): %w", err)
	}

	for {
		next, err := src.Next()
		if err != nil {
			return nil, fmt.Errorf("src.Next(): %w", err)
		}
		if !next {
			break
		}
		err = dest.Insert()
		if err != nil {
			return nil, fmt.Errorf("dest.Insert(): %w", err)
		}

		for _, fieldName := range sch.Fields() {
			val, err := src.GetVal(fieldName)
			if err != nil {
				return nil, fmt.Errorf("src.GetVal(%s): %w", fieldName, err)
			}
			err = dest.SetVal(fieldName, val)
			if err != nil {
				return nil, fmt.Errorf("dest.SetVal(%s, %v): %w", fieldName, val, err)
			}
		}
	}
	err = dest.BeforeFirst()
	if err != nil {
		return nil, fmt.Errorf("dest.BeforeFirst(): %w", err)
	}

	return dest, nil
}

func (p *MaterializePlan) BlocksAccessed() int {
	// create a dummy Layout object to calculate slot size
	layout := record.NewLayoutFromSchema(p.srcPlan.Schema())
	rpb := float64(p.tx.BlockSize()) / float64(layout.SlotSize())
	return int(math.Ceil(float64(p.srcPlan.RecordsOutput()) / rpb))
}

func (p *MaterializePlan) RecordsOutput() int {
	return p.srcPlan.RecordsOutput()
}

func (p *MaterializePlan) DistinctValues(fieldName string) int {
	return p.srcPlan.DistinctValues(fieldName)
}

func (p *MaterializePlan) Schema() *record.Schema {
	return p.srcPlan.Schema()
}