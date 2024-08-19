package driver

import (
	"database/sql"
	"testing"
	// 異なるパッケージからドライバーを利用する場合は、init()を呼び出すためにインポートする必要がある
	// _"simpledb/driver"
)

func TestDriver(t *testing.T) {
	t.Logf("testing driver")
	db, err := sql.Open("simpledb", "playerdb")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	createTable(t, db, "create table player (player_id int, name varchar(10), birth_year int, country varchar(10), point int)")
	err = tx2.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx5, _ := db.Begin()
	insert(t, db, "insert into player (player_id, name, birth_year, country, point) values (1, 'Nobak', 1987, 'Serbia', 11055)")
	insert(t, db, "insert into player (player_id, name, birth_year, country, point) values (2, 'Carlos', 2003, 'Spain', 8855)")
	insert(t, db, "insert into player (player_id, name, birth_year, country, point) values (3, 'Daniil', 1996, 'Russia', 7555)")
	insert(t, db, "insert into player (player_id, name, birth_year, country, point) values (4, 'Jannik', 2001, 'Italy', 6490)")
	err = tx5.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx6, _ := db.Begin()
	queryPlayer(t, db)
	update(t, db, "update player set point = 8360 where player_id = 1")
	update(t, db, "update player set point = 8130 where player_id = 2")
	update(t, db, "update player set point = 6445 where player_id = 3")
	update(t, db, "update player set point = 9890 where player_id = 4")
	err = tx6.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx7, _ := db.Begin()
	queryPlayer(t, db)
	update(t, db, "update player set point = 0")
	err = tx7.Rollback()
	if err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	tx8, _ := db.Begin()
	queryPlayer(t, db)
	delete(t, db, "delete from player")
	err = tx8.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	defer db.Close()
}

func createTable(t *testing.T, db *sql.DB, cmd string) {
	t.Log("creating table")
	_, err := db.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	t.Log("created table")
}

func insert(t *testing.T, db *sql.DB, cmd string) {
	result, err := db.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to insert into table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("inserted %d rows", rows)
}

func update(t *testing.T, db *sql.DB, cmd string) {
	result, err := db.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to update table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("updated %d rows", rows)
}

func delete(t *testing.T, db *sql.DB, cmd string) {
	result, err := db.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to delete from table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("deleted %d rows", rows)
}

func queryPlayer(t *testing.T, db *sql.DB) {
	rows, err := db.Query("select player_id, name, birth_year, country, point from player")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var playerID int
		var name string
		var birthYear int
		var country string
		var point int
		err = rows.Scan(&playerID, &name, &birthYear, &country, &point)
		if err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		t.Logf("player {player_id: %d, name: %s, birth_year: %d, country: %s, point: %d}", playerID, name, birthYear, country, point)
	}
}
