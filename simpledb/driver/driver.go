package driver

import (
	"database/sql"
	"database/sql/driver"

	"simpledb/server"
)

type SimpleDBDriver struct{}

func init() {
	sql.Register("simpledb", &SimpleDBDriver{})
}

func (d SimpleDBDriver) Open(name string) (driver.Conn, error) {
	db, err := server.NewSimpleDBWithMetadata(name)
	if err != nil {
		return nil, err
	}
	tx, err := db.NewTx()
	if err != nil {
		return nil, err
	}
	return &Connection{
		db:          db,
		transaction: tx,
		planner:     db.Planner(),
	}, nil
}
