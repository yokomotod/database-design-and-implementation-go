package query

import (
	"fmt"
	"simpledb/record"
)

var _ Scan = (*SortScan)(nil)

type SortScan struct {
	s1, s2             UpdateScan
	currentScan        UpdateScan
	comp               *RecordComparator
	hasMore1, hasMore2 bool
	savedPosition      []*record.RID
}

func NewSortScan(runs []*TempTable, comp *RecordComparator) (*SortScan, error) {
	s1, err := runs[0].Open()
	if err != nil {
		return nil, fmt.Errorf("runs[0].Open: %w", err)
	}
	hasMore1, err := s1.Next()
	if err != nil {
		return nil, fmt.Errorf("s1.Next: %w", err)
	}

	var s2 UpdateScan
	var hasMore2 bool
	if len(runs) > 1 {
		s2, err = runs[1].Open()
		if err != nil {
			return nil, fmt.Errorf("runs[1].Open: %w", err)
		}
		hasMore2, err = s2.Next()
		if err != nil {
			return nil, fmt.Errorf("s2.Next: %w", err)
		}
	}

	return &SortScan{
		s1:          s1,
		s2:          s2,
		currentScan: nil,
		comp:        comp,
		hasMore1:    hasMore1,
		hasMore2:    hasMore2,
	}, nil
}

func (ss *SortScan) BeforeFirst() error {
	var err error
	if err = ss.s1.BeforeFirst(); err != nil {
		return fmt.Errorf("ss.s1.BeforeFirst: %w", err)
	}
	ss.hasMore1, err = ss.s1.Next()
	if err != nil {
		return fmt.Errorf("ss.s1.Next: %w", err)
	}

	if ss.s2 != nil {
		if err = ss.s2.BeforeFirst(); err != nil {
			return fmt.Errorf("ss.s2.BeforeFirst: %w", err)
		}
		ss.hasMore2, err = ss.s2.Next()
		if err != nil {
			return fmt.Errorf("ss.s2.Next: %w", err)
		}
	}

	return nil
}

func (ss *SortScan) Next() (bool, error) {
	var err error

	if ss.currentScan == ss.s1 {
		ss.hasMore1, err = ss.s1.Next()
		if err != nil {
			return false, fmt.Errorf("ss.s1.Next: %w", err)
		}
	} else if ss.s2 != nil && ss.currentScan == ss.s2 {
		ss.hasMore2, err = ss.s2.Next()
		if err != nil {
			return false, fmt.Errorf("ss.s2.Next: %w", err)
		}
	}

	if !ss.hasMore1 && !ss.hasMore2 {
		return false, nil
	} else if ss.hasMore1 && ss.hasMore2 {
		cmp, err := ss.comp.Compare(ss.s1, ss.s2)
		if err != nil {
			return false, fmt.Errorf("ss.comp.Compare: %w", err)
		}
		if cmp < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}

	return true, nil
}

func (ss *SortScan) Close() {
	ss.s1.Close()
	if ss.s2 != nil {
		ss.s2.Close()
	}
}

func (ss *SortScan) GetVal(fieldName string) (*Constant, error) {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *SortScan) GetInt(fieldName string) (int32, error) {
	return ss.currentScan.GetInt(fieldName)
}

func (ss *SortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

func (ss *SortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *SortScan) SavePosition() error {
	rid1, err := ss.s1.GetRID()
	if err != nil {
		return fmt.Errorf("ss.s1.GetRID: %w", err)
	}

	if ss.s2 != nil {
		rid2, err := ss.s2.GetRID()
		if err != nil {
			return fmt.Errorf("ss.s2.GetRID: %w", err)
		}
		ss.savedPosition = []*record.RID{rid1, rid2}
	} else {
		ss.savedPosition = []*record.RID{rid1}
	}

	return nil
}

func (ss *SortScan) RestorePosition() error {
	rid1 := ss.savedPosition[0]
	if err := ss.s1.MoveToRID(rid1); err != nil {
		return fmt.Errorf("ss.s1.MoveToRID: %w", err)
	}

	if len(ss.savedPosition) > 1 {
		rid2 := ss.savedPosition[1]
		if err := ss.s2.MoveToRID(rid2); err != nil {
			return fmt.Errorf("ss.s2.MoveToRID: %w", err)
		}
	}

	return nil
}
