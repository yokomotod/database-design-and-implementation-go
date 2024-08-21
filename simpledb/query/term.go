package query

import (
	"fmt"
	"math"
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

func (t *Term) reductionFactor(p planLike) int {
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName := t.lhs.AsFieldName()
		rhsName := t.rhs.AsFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}
	if t.lhs.IsFieldName() {
		return p.DistinctValues(t.lhs.AsFieldName())
	}
	if t.rhs.IsFieldName() {
		return p.DistinctValues(t.rhs.AsFieldName())
	}
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}

	return math.MaxInt

}

// Determine if this term is of the form "F=c"
// where F is the specified field and c is some constant.
// If so, the method returns that constant.
// If not, the method returns null.
func (t *Term) equatesWithConstant(fieldName string) *Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	} else {
		return nil
	}
}

// Determine if this term is of the form "F1=F2"
// where F1 is the specified field and F2 is another field.
// If so, the method returns the name of that field.
// If not, the method returns empty string.
func (t *Term) equatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	} else {
		return ""
	}
}
