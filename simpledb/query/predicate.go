package query

import "strings"

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
