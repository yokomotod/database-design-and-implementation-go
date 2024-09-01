package query

import (
	"fmt"
	"strings"
)

var _ AggregationFn = (*MaxFn)(nil)

type MaxFn struct {
	fieldName string
	val       *Constant
}

func NewMaxFn(fieldName string) *MaxFn {
	return &MaxFn{fieldName: fieldName, val: nil}
}

func (mf *MaxFn) ProcessFirst(scan Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return fmt.Errorf("scan.GetVal(): %v", err)
	}
	mf.val = val
	return nil
}

func (mf *MaxFn) ProcessNext(scan Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return fmt.Errorf("scan.GetVal(): %v", err)
	}
	if strings.Compare(val.String(), mf.val.String()) > 0 {
		mf.val = val
	}
	return nil
}

func (mf *MaxFn) FieldName() string {
	return fmt.Sprintf("max(%s)", mf.fieldName)
}

func (mf *MaxFn) Value() *Constant {
	return mf.val
}
