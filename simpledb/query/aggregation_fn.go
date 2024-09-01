package query

type AggregationFn interface {
	ProcessFirst(scan Scan) error
	ProcessNext(scan Scan) error
	FieldName() string
	Value() *Constant
}
