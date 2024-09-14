package query

import "fmt"

type GroupValue struct {
	vals map[string]*Constant
}

func NewGroupValue(scan Scan, fields []string) (*GroupValue, error) {
	vals := make(map[string]*Constant)
	for _, field := range fields {
		val, err := scan.GetVal(field)
		if err != nil {
			return nil, fmt.Errorf("scan.GetVal(%s): %v", field, err)
		}
		vals[field] = val
	}
	return &GroupValue{vals: vals}, nil
}

func (gv *GroupValue) GetVal(fieldName string) (*Constant, error) {
	val, ok := gv.vals[fieldName]
	if !ok {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}
	return val, nil
}

func (gv *GroupValue) Equals(other *GroupValue) bool {
	if len(gv.vals) != len(other.vals) {
		return false
	}
	for fieldName, val := range gv.vals {
		otherVal, ok := other.vals[fieldName]
		if !ok {
			return false
		}
		if !val.Equals(otherVal) {
			return false
		}
	}
	return true
}
