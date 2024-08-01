package query

import "simpledb/record"

type UpdateScan interface {
	SetVal(fieldName string, val *record.Constant) error
	SetInt(fieldName string, val int32) error
	SetString(fieldName string, val string) error
	Insert() error
	Delete() error
	GetRID() *record.RID
	MoveToRID(rid *record.RID)
}
