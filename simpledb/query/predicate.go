package query

import (
	"math"
	"simpledb/record"
	"strings"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{
		terms: []*Term{},
	}
}

func NewPredicateWithTerm(t *Term) *Predicate {
	return &Predicate{
		terms: []*Term{t},
	}
}

func (p *Predicate) ConjoinWith(other *Predicate) {
	p.terms = append(p.terms, other.terms...)
}

func (p *Predicate) IsSatisfied(scan Scan) (bool, error) {
	for _, term := range p.terms {
		isSatisfied, err := term.IsSatisfied(scan)
		if err != nil {
			return false, err
		}
		if !isSatisfied {
			return false, nil
		}
	}
	return true, nil
}

func (p *Predicate) String() string {
	var terms []string
	for _, term := range p.terms {
		terms = append(terms, term.String())
	}
	return strings.Join(terms, " and ")
}

// plan.Plan インターフェースの一部を要求する
type planLike interface {
	DistinctValues(fieldName string) int32
}

func (p *Predicate) ReductionFactor(plan planLike) int32 {
	reductionFactor := int32(1)
	for _, term := range p.terms {
		rf := term.reductionFactor(plan)
		if rf == math.MaxInt32 {
			return math.MaxInt32
		}
		reductionFactor *= rf
	}
	return reductionFactor
}

// Return the subpredicate that applies to the specified schema
func (p *Predicate) SelectSubPred(schema *record.Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if !term.AppliesTo(schema) {
			continue
		}

		result.terms = append(result.terms, term)
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

// Return the subpredicate consisting of terms that apply
// to the union of the two specified schemas,
// but not to either schema separately
func (p *Predicate) JoinSubPred(sch1, sch2 *record.Schema) *Predicate {
	result := NewPredicate()
	newSch := record.NewSchema()
	newSch.AddAll(sch1)
	newSch.AddAll(sch2)

	for _, term := range p.terms {
		if term.AppliesTo(sch1) || term.AppliesTo(sch2) || !term.AppliesTo(newSch) {
			continue
		}
		result.terms = append(result.terms, term)
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

func (p *Predicate) EquatesWithConstant(fieldName string) *Constant {
	for _, term := range p.terms {
		c := term.equatesWithConstant(fieldName)
		if c != nil {
			return c
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		f := term.equatesWithField(fieldName)
		if f != "" {
			return f
		}
	}
	return ""
}
