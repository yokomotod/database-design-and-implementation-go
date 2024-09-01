package driver

import (
	"database/sql"
	"path"
	"testing"
	// 異なるパッケージからドライバーを利用する場合は、init()を呼び出すためにインポートする必要がある
	// _"simpledb/driver"
)

func TestDriver(t *testing.T) {
	t.Logf("testing driver")
	db, err := sql.Open("simpledb", path.Join(t.TempDir(), "playerdb"))
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	tx2 := beginTx(t, db)
	createTable(t, tx2, "create table player (player_id int, name varchar(10), birth_year int, country varchar(10), point int)")
	commit(t, tx2)

	tx3 := beginTx(t, db)
	insert(t, tx3, "insert into player (player_id, name, birth_year, country, point) values (1, 'Nobak', 1987, 'Serbia', 11055)")
	insert(t, tx3, "insert into player (player_id, name, birth_year, country, point) values (2, 'Carlos', 2003, 'Spain', 8855)")
	insert(t, tx3, "insert into player (player_id, name, birth_year, country, point) values (3, 'Daniil', 1996, 'Russia', 7555)")
	insert(t, tx3, "insert into player (player_id, name, birth_year, country, point) values (4, 'Jannik', 2001, 'Italy', 6490)")
	commit(t, tx3)

	tx4 := beginTx(t, db)
	points3 := queryPlayer(t, tx4)
	expected3 := []int{11055, 8855, 7555, 6490}
	for i, s := range points3 {
		if s != expected3[i] {
			t.Errorf("expected: %d, but got: %d", expected3[i], points3[i])
		}
	}
	update(t, tx4, "update player set point = 8360 where player_id = 1")
	update(t, tx4, "update player set point = 8130 where player_id = 2")
	update(t, tx4, "update player set point = 6445 where player_id = 3")
	update(t, tx4, "update player set point = 9890 where player_id = 4")
	commit(t, tx4)

	tx5 := beginTx(t, db)
	points4 := queryPlayer(t, tx5)
	expected4 := []int{8360, 8130, 6445, 9890}
	for i, s := range points4 {
		if s != expected4[i] {
			t.Errorf("expected: %d, but got: %d", expected4[i], points4[i])
		}
	}
	update(t, tx5, "update player set point = 0")
	points5 := queryPlayer(t, tx5)
	expected5 := []int{0, 0, 0, 0}
	for i, s := range points5 {
		if s != expected5[i] {
			t.Errorf("expected: %d, but got: %d", expected5[i], points5[i])
		}
	}
	rollback(t, tx5)

	tx6 := beginTx(t, db)
	points6 := queryPlayer(t, tx6)
	expected6 := []int{8360, 8130, 6445, 9890}
	for i, s := range points6 {
		if s != expected6[i] {
			t.Errorf("expected: %d, but got: %d", expected6[i], points6[i])
		}
	}
	rows6 := delete(t, tx6, "delete from player")
	if rows6 != 4 {
		t.Errorf("expected 4 rows affected, but got %d", rows6)
	}
	commit(t, tx6)
}

func beginTx(t *testing.T, db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	return tx
}

func commit(t *testing.T, tx *sql.Tx) {
	err := tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func rollback(t *testing.T, tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}
}

func createTable(t *testing.T, tx *sql.Tx, cmd string) {
	_, err := tx.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	t.Log("created table")
}

func insert(t *testing.T, tx *sql.Tx, cmd string) {
	result, err := tx.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to insert into table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("inserted %d rows", rows)
}

func update(t *testing.T, tx *sql.Tx, cmd string) {
	result, err := tx.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to update table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("updated %d rows", rows)
}

func delete(t *testing.T, tx *sql.Tx, cmd string) int64 {
	result, err := tx.Exec(cmd)
	if err != nil {
		t.Fatalf("failed to delete from table: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	t.Logf("deleted %d rows", rows)
	return rows
}

func queryPlayer(t *testing.T, tx *sql.Tx) []int {
	rows, err := tx.Query("select player_id, name, birth_year, country, point from player")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()
	points := []int{}
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
		points = append(points, point)
	}
	return points
}
