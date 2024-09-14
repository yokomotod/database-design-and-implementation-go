package query

import (
	"fmt"
)

var _ AggregationFn = (*MinFn)(nil)

type MinFn struct {
	fieldName string
	val       *Constant
}

func NewMinFn(fieldName string) *MinFn {
	return &MinFn{fieldName: fieldName, val: nil}
}

func (mf *MinFn) ProcessFirst(scan Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return fmt.Errorf("scan.GetVal(): %v", err)
	}
	mf.val = val
	return nil
}

func (mf *MinFn) ProcessNext(scan Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return fmt.Errorf("scan.GetVal(): %v", err)
	}
	cmp, err := val.CompareTo(mf.val)
	if err != nil {
		return fmt.Errorf("val.CompareTo(): %v", err)
	}
	if cmp < 0 {
		mf.val = val
	}
	return nil
}

func (mf *MinFn) FieldName() string {
	return fmt.Sprintf("min(%s)", mf.fieldName)
}

func (mf *MinFn) Value() *Constant {
	return mf.val
}
