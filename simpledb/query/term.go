package query

import (
	"fmt"
	"simpledb/record"
)

type Term struct {
	lhs *Expression
	rhs *Expression
}

func NewTerm(lhs *Expression, rhs *Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

func (t *Term) IsSatisfied(scan Scan) (bool, error) {
	lhsVal, err := t.lhs.Evaluate(scan)
	if err != nil {
		return false, err
	}
	rhsVal, err := t.rhs.Evaluate(scan)
	if err != nil {
		return false, err
	}
	return rhsVal.Equals(lhsVal), nil
}

func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs, t.rhs)
}
