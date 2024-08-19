package driver

import (
	"context"
	"database/sql/driver"

	"simpledb/plan"
	"simpledb/server"
	"simpledb/tx"
)

type Connection struct {
	db          *server.SimpleDB
	transaction *tx.Transaction
	planner     *plan.Planner
}

func (conn *Connection) Begin() (driver.Tx, error) {
	panic("unimplemented")
}

func (conn *Connection) Close() error {
	return conn.transaction.Commit()
}

func (conn *Connection) Prepare(query string) (driver.Stmt, error) {
	panic("unimplemented")
}

func (conn *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rows, err := conn.planner.ExecuteUpdate(query, conn.transaction)
	if err != nil {
		return nil, err
	}
	return &Result{rowsAffected: rows}, nil
}

func (conn *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	plan, err := conn.planner.CreateQueryPlan(query, conn.transaction)
	if err != nil {
		return nil, err
	}
	scan, err := plan.Open()
	if err != nil {
		return nil, err
	}
	return &Rows{schema: *plan.Schema(), scan: scan}, nil
}
