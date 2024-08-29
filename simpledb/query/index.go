package query

import (
	"simpledb/record"
)

type Index interface {
	// Index を検索対象のデータを指し示す位置まで進める
	BeforeFirst(searchkey *Constant) error
	// Index が指し示すデータを取得する、取得対象がなくなると false を返す
	Next() (bool, error)
	// Index が指し示すデータの位置を取得する。この位置から実際のデータを取得することができる
	GetDataRID() (*record.RID, error)
	Insert(dataval *Constant, datarid *record.RID) error
	Delete(dataval *Constant, datarid *record.RID) error
	Close() error
}
