package query

import (
	"fmt"
)

var _ AggregationFn = (*CountFn)(nil)

type CountFn struct {
	fieldName string
	count     int32
}

func NewCountFn(fieldName string) *CountFn {
	return &CountFn{fieldName: fieldName, count: 0}
}

func (mf *CountFn) ProcessFirst(scan Scan) error {
	mf.count = 1
	return nil
}

func (mf *CountFn) ProcessNext(scan Scan) error {
	mf.count++
	return nil
}

func (mf *CountFn) FieldName() string {
	return fmt.Sprintf("count(%s)", mf.fieldName)
}

func (mf *CountFn) Value() *Constant {
	return NewConstantWithInt(mf.count)
}
