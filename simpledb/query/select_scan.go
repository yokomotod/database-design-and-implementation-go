package query

import (
	"errors"
	"simpledb/record"
)

var ErrNotUpdatable = errors.New("scan is not updatable")

type SelectScan struct {
	scan Scan
	pred *Predicate
}

func NewSelectScan(scan Scan, pred *Predicate) *SelectScan {
	return &SelectScan{
		scan: scan,
		pred: pred,
	}
}

// Scan methods
func (ss *SelectScan) BeforeFirst() error {
	return ss.scan.BeforeFirst()
}

func (ss *SelectScan) Next() (bool, error) {
	next, err := ss.scan.Next()
	if err != nil {
		return false, err
	}
	for next {
		ok, err := ss.pred.IsSatisfied(ss.scan)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		next, err = ss.scan.Next()
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func (ss *SelectScan) GetInt(fieldName string) (int32, error) {
	return ss.scan.GetInt(fieldName)
}

func (ss *SelectScan) GetString(fieldName string) (string, error) {
	return ss.scan.GetString(fieldName)
}

func (ss *SelectScan) GetVal(fieldName string) (*Constant, error) {
	return ss.scan.GetVal(fieldName)
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.scan.HasField(fieldName)
}

func (ss *SelectScan) Close() {
	ss.scan.Close()
}

// Update scan methods
func (ss *SelectScan) SetInt(fieldName string, val int32) error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.SetInt(fieldName, val)
}

func (ss *SelectScan) SetString(fieldName string, val string) error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.SetString(fieldName, val)
}

func (ss *SelectScan) SetVal(fieldName string, val *Constant) error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.SetVal(fieldName, val)
}

func (ss *SelectScan) Insert() error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.Insert()
}

func (ss *SelectScan) Delete() error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.Delete()
}

func (ss *SelectScan) GetRID() (*record.RID, error) {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return nil, ErrNotUpdatable
	}
	return us.GetRID()
}

func (ss *SelectScan) MoveToRID(rid *record.RID) error {
	us, ok := ss.scan.(UpdateScan)
	if !ok {
		return ErrNotUpdatable
	}
	return us.MoveToRID(rid)
}
