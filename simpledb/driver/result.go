package driver

type Result struct {
	rowsAffected int
}

func NewResult(rowsAffected int) *Result {
	return &Result{rowsAffected: rowsAffected}
}

func (res *Result) LastInsertId() (int64, error) {
	panic("unimplemented")
}

func (res *Result) RowsAffected() (int64, error) {
	return int64(res.rowsAffected), nil
}
