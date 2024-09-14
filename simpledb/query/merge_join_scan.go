package query

import (
	"fmt"
	"strings"
)

var _ Scan = (*MergeJoinScan)(nil)

type MergeJoinScan struct {
	s1, s2                 *SortScan
	fieldName1, fieldName2 string
	joinVal                *Constant
}

func NewMergeJoinScan(s1, s2 *SortScan, fieldName1, fieldName2 string) *MergeJoinScan {
	return &MergeJoinScan{
		s1:         s1,
		s2:         s2,
		fieldName1: fieldName1,
		fieldName2: fieldName2,
	}
}

func (mjs *MergeJoinScan) Close() {
	mjs.s1.Close()
	mjs.s2.Close()
}

func (mjs *MergeJoinScan) BeforeFirst() error {
	err := mjs.s1.BeforeFirst()
	if err != nil {
		return fmt.Errorf("mjs.s1.BeforeFirst(): %v", err)
	}
	err = mjs.s2.BeforeFirst()
	if err != nil {
		return fmt.Errorf("mjs.s2.BeforeFirst(): %v", err)
	}
	return nil
}

func (mjs *MergeJoinScan) Next() (bool, error) {
	var v1, v2 *Constant
	hasMore2, err := mjs.s2.Next()
	if err != nil {
		return false, fmt.Errorf("mjs.s2.Next(): %v", err)
	}
	if hasMore2 {
		v2, err = mjs.s2.GetVal(mjs.fieldName2)
		if err != nil {
			return false, fmt.Errorf("mjs.s2.GetVal(%s): %v", mjs.fieldName2, err)
		}
		if mjs.joinVal != nil && v2.Equals(mjs.joinVal) {
			return true, nil
		}
	}

	hasMore1, err := mjs.s1.Next()
	if err != nil {
		return false, fmt.Errorf("mjs.s1.Next(): %v", err)
	}
	if hasMore1 {
		v1, err = mjs.s1.GetVal(mjs.fieldName1)
		if err != nil {
			return false, fmt.Errorf("mjs.s1.GetVal(%s): %v", mjs.fieldName1, err)
		}
		if mjs.joinVal != nil && v1.Equals(mjs.joinVal) {
			err := mjs.s2.RestorePosition()
			if err != nil {
				return false, fmt.Errorf("mjs.s2.RestorePosition(): %v", err)
			}
			return true, nil
		}
	}

	for hasMore1 && hasMore2 {
		v1, err = mjs.s1.GetVal(mjs.fieldName1)
		if err != nil {
			return false, fmt.Errorf("mjs.s1.GetVal(%s): %v", mjs.fieldName1, err)
		}
		v2, err = mjs.s2.GetVal(mjs.fieldName2)
		if err != nil {
			return false, fmt.Errorf("mjs.s2.GetVal(%s): %v", mjs.fieldName2, err)
		}

		cmp := strings.Compare(v1.String(), v2.String())
		if cmp < 0 {
			hasMore1, err = mjs.s1.Next()
			if err != nil {
				return false, fmt.Errorf("mjs.s1.Next(): %v", err)
			}
		} else if cmp > 0 {
			hasMore2, err = mjs.s2.Next()
			if err != nil {
				return false, fmt.Errorf("mjs.s2.Next(): %v", err)
			}
		} else {
			err := mjs.s2.SavePosition()
			if err != nil {
				return false, fmt.Errorf("mjs.s2.SavePosition(): %v", err)
			}
			val, err := mjs.s2.GetVal(mjs.fieldName2)
			if err != nil {
				return false, fmt.Errorf("mjs.s2.GetVal(%s): %v", mjs.fieldName2, err)
			}
			mjs.joinVal = val
			return true, nil
		}
	}

	return false, nil
}

func (mjs *MergeJoinScan) GetInt(fieldName string) (int32, error) {
	if mjs.s1.HasField(fieldName) {
		return mjs.s1.GetInt(fieldName)
	}
	return mjs.s2.GetInt(fieldName)
}

func (mjs *MergeJoinScan) GetString(fieldName string) (string, error) {
	if mjs.s1.HasField(fieldName) {
		return mjs.s1.GetString(fieldName)
	}
	return mjs.s2.GetString(fieldName)
}

func (mjs *MergeJoinScan) GetVal(fieldName string) (*Constant, error) {
	if mjs.s1.HasField(fieldName) {
		return mjs.s1.GetVal(fieldName)
	}
	return mjs.s2.GetVal(fieldName)
}

func (mjs *MergeJoinScan) HasField(fieldName string) bool {
	return mjs.s1.HasField(fieldName) || mjs.s2.HasField(fieldName)
}
