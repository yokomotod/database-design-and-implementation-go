package query

import (
	"fmt"
	"slices"
)

var _ Scan = (*GroupByScan)(nil)

type GroupByScan struct {
	scan          *SortScan
	groupFields   []string
	aggFns        []AggregationFn
	groupValue    *GroupValue
	hasMoreGroups bool
}

func NewGroupByScan(scan *SortScan, groupFields []string, aggFns []AggregationFn) *GroupByScan {
	return &GroupByScan{
		scan:          scan,
		groupFields:   groupFields,
		aggFns:        aggFns,
		groupValue:    nil,
		hasMoreGroups: true,
	}
}

func (gs *GroupByScan) BeforeFirst() error {
	err := gs.scan.BeforeFirst()
	if err != nil {
		return fmt.Errorf("gs.scan.BeforeFirst: %w", err)
	}
	next, err := gs.scan.Next()
	if err != nil {
		return fmt.Errorf("gs.scan.Next: %w", err)
	}
	gs.hasMoreGroups = next
	return nil
}

func (gs *GroupByScan) Next() (bool, error) {
	if !gs.hasMoreGroups {
		return false, nil
	}

	var err error
	for _, fn := range gs.aggFns {
		err := fn.ProcessFirst(gs.scan)
		if err != nil {
			return false, fmt.Errorf("fn.ProcessFirst: %w", err)
		}
	}
	gs.groupValue, err = NewGroupValue(gs.scan, gs.groupFields)
	if err != nil {
		return false, fmt.Errorf("NewGroupValue: %w", err)
	}
	for {
		hasMore, err := gs.scan.Next()
		gs.hasMoreGroups = hasMore
		if err != nil {
			return false, fmt.Errorf("gs.scan.Next: %w", err)
		}
		if !hasMore {
			break
		}
		gv, err := NewGroupValue(gs.scan, gs.groupFields)
		if err != nil {
			return false, fmt.Errorf("NewGroupValue: %w", err)
		}
		if !gs.groupValue.Equals(gv) {
			break
		}
		for _, fn := range gs.aggFns {
			err := fn.ProcessNext(gs.scan)
			if err != nil {
				return false, fmt.Errorf("fn.ProcessNext: %w", err)
			}
		}
	}

	return true, nil
}

func (gs *GroupByScan) Close() {
	gs.scan.Close()
}

func (gs *GroupByScan) GetVal(fieldName string) (*Constant, error) {
	if slices.Contains(gs.groupFields, fieldName) {
		val, err := gs.groupValue.GetVal(fieldName)
		if err != nil {
			return nil, fmt.Errorf("gs.groupValue.GetVal(%s): %w", fieldName, err)
		}
		return val, nil
	}
	for _, fn := range gs.aggFns {
		if fieldName == fn.FieldName() {
			return fn.Value(), nil
		}
	}
	return nil, fmt.Errorf("field %s not found", fieldName)
}

func (gs *GroupByScan) GetInt(fieldName string) (int32, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return 0, fmt.Errorf("gs.GetVal(%s): %w", fieldName, err)
	}
	intVal, err := val.AsInt()
	if err != nil {
		return 0, fmt.Errorf("val.AsInt: %w", err)
	}
	return intVal, nil
}

func (gs *GroupByScan) GetString(fieldName string) (string, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return "", fmt.Errorf("gs.GetVal(%s): %w", fieldName, err)
	}
	strVal, err := val.AsString()
	if err != nil {
		return "", fmt.Errorf("val.AsString: %w", err)
	}
	return strVal, nil
}

func (gs *GroupByScan) HasField(fieldName string) bool {
	if slices.Contains(gs.groupFields, fieldName) {
		return true
	}
	for _, fn := range gs.aggFns {
		if fieldName == fn.FieldName() {
			return true
		}
	}
	return false
}
