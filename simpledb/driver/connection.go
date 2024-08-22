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
	transaction *TransactionWithConnection
	planner     *plan.Planner
}

func (conn *Connection) Ping() error {
	return nil
}

func (conn *Connection) Begin() (driver.Tx, error) {
	panic("unimplemented")
}

func (conn *Connection) Close() error {
	return nil
}

func (conn *Connection) Prepare(query string) (driver.Stmt, error) {
	panic("unimplemented")
}

func (conn *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rows, err := conn.planner.ExecuteUpdate(query, conn.transaction.tx)
	if err != nil {
		return nil, err
	}
	return &Result{rowsAffected: rows}, nil
}

func (conn *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	plan, err := conn.planner.CreateQueryPlan(query, conn.transaction.tx)
	if err != nil {
		return nil, err
	}
	scan, err := plan.Open()
	if err != nil {
		return nil, err
	}
	return &Rows{schema: *plan.Schema(), scan: scan}, nil
}

func (conn *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := conn.db.NewTx()
	if err != nil {
		return nil, err
	}
	conn.transaction = &TransactionWithConnection{tx: tx, conn: conn}
	return conn.transaction, nil
}

type TransactionWithConnection struct {
	tx   *tx.Transaction
	conn *Connection
}

func (txc *TransactionWithConnection) Commit() error {
	err := txc.tx.Commit()
	if err != nil {
		return err
	}
	txc.conn.transaction = nil
	return nil
}

func (txc *TransactionWithConnection) Rollback() error {
	err := txc.tx.Rollback()
	if err != nil {
		return err
	}
	txc.conn.transaction = nil
	return nil
}
