package driver

import (
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
	conn := NewConnection(db, db.Planner())
	fmt.Println("database opened")
	return conn, nil
}
