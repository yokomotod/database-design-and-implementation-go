package query

import "simpledb/record"

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt(fieldName string) (int32, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (*record.Constant, error)
	HasField(fieldName string) bool
	Close()
}
