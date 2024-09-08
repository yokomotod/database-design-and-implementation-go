package plan

import (
	"simpledb/query"
	"simpledb/record"
)

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int32
	RecordsOutput() int32
	DistinctValues(fieldName string) int32
	Schema() *record.Schema
}
