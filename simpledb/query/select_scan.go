package query

import "simpledb/record"

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

func (ss *SelectScan) GetVal(fieldName string) (*record.Constant, error) {
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
	us := ss.scan.(UpdateScan)
	return us.SetInt(fieldName, val)
}

func (ss *SelectScan) SetString(fieldName string, val string) error {
	us := ss.scan.(UpdateScan)
	return us.SetString(fieldName, val)
}

func (ss *SelectScan) SetVal(fieldName string, val *record.Constant) error {
	us := ss.scan.(UpdateScan)
	return us.SetVal(fieldName, val)
}

func (ss *SelectScan) Insert() error {
	us := ss.scan.(UpdateScan)
	return us.Insert()
}

func (ss *SelectScan) Delete() error {
	us := ss.scan.(UpdateScan)
	return us.Delete()
}

func (ss *SelectScan) GetRID() *record.RID {
	us := ss.scan.(UpdateScan)
	return us.GetRID()
}

func (ss *SelectScan) MoveToRID(rid *record.RID) {
	us := ss.scan.(UpdateScan)
	us.MoveToRID(rid)
}
