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
	DistinctValues(fieldName string) int
}

func (p *Predicate) ReductionFactor(plan planLike) int {
	reductionFactor := 1
	for _, term := range p.terms {
		rf := term.reductionFactor(plan)
		if rf == math.MaxInt {
			return math.MaxInt
		}
		reductionFactor *= rf
	}
	return reductionFactor
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
