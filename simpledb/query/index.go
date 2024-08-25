package query

import (
	"simpledb/record"
)

type Index interface {
	BeforeFirst(searchkey *Constant) error
	Next() (bool, error)
	GetDataRID() (*record.RID, error)
	Insert(dataval *Constant, datarid *record.RID) error
	Delete(dataval *Constant, datarid *record.RID) error
	Close() error
}
