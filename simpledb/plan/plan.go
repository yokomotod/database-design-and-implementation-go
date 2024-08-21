package plan

import (
	"simpledb/query"
	"simpledb/record"
)

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	Schema() *record.Schema
}
