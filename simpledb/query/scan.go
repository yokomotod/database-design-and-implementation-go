package query

import "simpledb/record"

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt(fieldName string) (int32, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (*Constant, error)
	HasField(fieldName string) bool
	Close()
}
type UpdateScan interface {
	SetVal(fieldName string, val *Constant) error
	SetInt(fieldName string, val int32) error
	SetString(fieldName string, val string) error
	Insert() error
	Delete() error
	GetRID() (*record.RID, error)
	MoveToRID(rid *record.RID) error
}
