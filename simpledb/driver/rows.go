package driver

import (
	"database/sql/driver"
	"io"

	"simpledb/query"
	"simpledb/record"
)

type Rows struct {
	schema record.Schema
	scan   query.Scan
}

func (r *Rows) Columns() []string {
	return r.schema.Fields()
}

func (r *Rows) Close() error {
	r.scan.Close()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	ok, err := r.scan.Next()
	if err != nil {
		return err
	}
	if !ok {
		return io.EOF
	}
	for i, col := range r.Columns() {
		val, err := r.scan.GetVal(col)
		if err != nil {
			return err
		}
		dest[i] = val.AnyValue()
	}
	return nil
}
