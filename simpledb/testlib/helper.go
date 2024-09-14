package testlib

import (
	"fmt"
	"simpledb/server"
	"testing"
)

func InsertTestData(t *testing.T, simpledb *server.SimpleDB) error {
	t.Helper()

	tx, err := simpledb.NewTx()
	if err != nil {
		return err
	}

	//create table
	planner := simpledb.Planner()

	_, err = planner.ExecuteUpdate("create table student(sid int, sname varchar(10), majorid int, gradyear int) ", tx)
	if err != nil {
		return err
	}
	_, err = planner.ExecuteUpdate("create index majorid_idx on student(majorid)", tx)
	if err != nil {
		return err
	}
	_, err = planner.ExecuteUpdate("create table dept(did int, dname varchar(8))", tx)
	if err != nil {
		return err
	}

	depts := []struct {
		did   int
		dname string
	}{
		{10, "compsci"},
		{20, "math"},
		{30, "drama"},
	}

	for _, d := range depts {
		query := fmt.Sprintf("insert into dept(did, dname) values(%d, '%s')", d.did, d.dname)
		_, err = planner.ExecuteUpdate(query, tx)
		if err != nil {
			return err
		}
	}

	students := []struct {
		sid      int
		sname    string
		majorid  int
		gradyear int
	}{
		{1, "joe", 10, 2021},
		{2, "amy", 20, 2020},
		{3, "max", 10, 2022},
		{4, "sue", 20, 2022},
		{5, "bob", 30, 2020},
		{6, "kim", 20, 2019},
		{7, "art", 30, 2021},
		{8, "pat", 10, 2022},
		{9, "lee", 10, 2021},
		{10, "dan", 20, 2020},
	}

	for _, s := range students {
		query := fmt.Sprintf("insert into student(sid, sname, majorid, gradyear) values(%d, '%s', %d, %d)", s.sid, s.sname, s.majorid, s.gradyear)
		_, err = planner.ExecuteUpdate(query, tx)
		if err != nil {
			return err
		}
	}

	// 強制的に統計情報を更新 (Metadata Managerに機能追加を検討)
	mdm := simpledb.MetadataManager()
	studentLayout, err := mdm.GetLayout("student", tx)
	if err != nil {
		return err
	}
	for i := 0; i <= 100; i++ {
		_, err = mdm.GetStatInfo("student", studentLayout, tx)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
