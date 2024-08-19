package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"simpledb/server"
)

type SimpleDBDriver struct{}

func init() {
	sql.Register("simpledb", &SimpleDBDriver{})
}

func (d SimpleDBDriver) Open(name string) (driver.Conn, error) {
	fmt.Println("opening database")
	db, err := server.NewSimpleDBWithMetadata(name)
	if err != nil {
		return nil, err
	}
	conn := &Connection{db: db, transaction: nil, planner: db.Planner()}
	_, err = conn.BeginTx(context.TODO(), driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	fmt.Println("database opened")
	return conn, nil
}
