package plan

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
)

type MultibufferProductPlan struct {
	logger *logger.Logger

	tx       *tx.Transaction
	lhs, rhs Plan
	schema   *record.Schema
}

func NewMultibufferProductPlan(tx *tx.Transaction, lhs Plan, rhs Plan) *MultibufferProductPlan {
	schema := record.NewSchema()
	schema.AddAll(lhs.Schema())
	schema.AddAll(rhs.Schema())

	return &MultibufferProductPlan{
		logger: logger.New("plan.MultibufferProductPlan", logger.Trace),

		tx:     tx,
		lhs:    NewMaterializePlan(tx, lhs),
		rhs:    rhs,
		schema: schema,
	}
}

/**
 * A scan for this query is created and returned, as follows.
 * First, the method materializes its LHS and RHS queries.
 * It then determines the optimal chunk size,
 * based on the size of the materialized RHS file and the
 * number of available buffers.
 * It creates a chunk plan for each chunk, saving them in a list.
 * Finally, it creates a multiscan for this list of plans,
 * and returns that scan.
 * @see simpledb.plan.Plan#open()
 */
func (p *MultibufferProductPlan) Open() (query.Scan, error) {
	leftscan, err := p.lhs.Open()
	if err != nil {
		return nil, fmt.Errorf("p.lhs.Open: %w", err)
	}

	tt, err := p.copyRecordsFrom(p.rhs)
	if err != nil {
		return nil, fmt.Errorf("p.copyRecordsFrom: %w", err)
	}

	return query.NewMultibufferProductScan(p.tx, leftscan, tt.TableName, tt.Layout)
}

func (p *MultibufferProductPlan) BlocksAccessed() int32 {
	avail := p.tx.AvailableBuffers()
	size := NewMaterializePlan(p.tx, p.rhs).BlocksAccessed()
	numchunks := size / avail // need `+ 1` ?

	blockAccessed := p.rhs.BlocksAccessed() + (p.lhs.BlocksAccessed() * numchunks)

	p.logger.Tracef("BlocksAccessed(): numchunks = size(%d) / avail(%d) = %d", size, avail, numchunks)
	p.logger.Tracef("BlocksAccessed() = rhs(%d) + (lhs(%d) * numchunks(%d)) = %d", p.rhs.BlocksAccessed(), p.lhs.BlocksAccessed(), numchunks, blockAccessed)
	return blockAccessed
}

func (p *MultibufferProductPlan) RecordsOutput() int32 {
	return p.lhs.RecordsOutput() * p.rhs.RecordsOutput()
}

func (p *MultibufferProductPlan) DistinctValues(fieldName string) int32 {
	if p.lhs.Schema().HasField(fieldName) {
		return p.lhs.DistinctValues(fieldName)
	}
	return p.rhs.DistinctValues(fieldName)
}

func (p *MultibufferProductPlan) Schema() *record.Schema {
	return p.schema
}

func (p *MultibufferProductPlan) copyRecordsFrom(plan Plan) (*query.TempTable, error) {
	src, err := plan.Open()
	if err != nil {
		return nil, fmt.Errorf("plan.Open: %w", err)
	}
	defer src.Close()

	sch := plan.Schema()

	t := query.NewTempTable(p.tx, sch)
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

	return t, nil
}
