package plan_test

import (
	"path"
	"simpledb/server"
	"testing"
)

func TestStudent(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "studentdb"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	planner:= simpleDB.Planner()

	_, err = planner.ExecuteUpdate("create table student (sid int, sname varchar(10), majorid int, gradyear int)", tx)
	if err !=nil{
		t.Fatalf("failed to create table: %v", err)
	}
	t.Logf("created table student")

	cnt, err := planner.ExecuteUpdate("insert into student(sid, sname, majorid) values (1, 'joe', 10, 2021)", tx)
	if err !=nil{
		t.Fatalf("failed to insert into table: %v", err)
	}
	t.Logf("insert into student: %d", cnt)

	cnt, err = planner.ExecuteUpdate("insert into student(sid, sname, majorid) values (2, 'amy', 20, 2020)", tx)
	if err !=nil{
		t.Fatalf("failed to insert into table: %v", err)
	}
	t.Logf("insert into student: %d", cnt)


	plan, err := planner.CreateQueryPlan("select sid, sname from student", tx)
	if err != nil {
		t.Fatalf("failed to create query plan: %v", err)
	}

	sc, err := plan.Open()
	if err != nil{
		t.Fatalf("failed to open scan: %v", err)
	}
	defer sc.Close()

	for {
		next, err := sc.Next()
		if err != nil{
			t.Fatalf("failed to get next: %v", err)
		}
		if !next{
			break
		}

		sid, err := sc.GetInt("sid")
		if err !=nil{
			t.Fatalf("failed to get int: %v", err)
		}
		sname, err := sc.GetString("sname")
		if err !=nil{
			t.Fatalf("failed to get string: %v", err)
		}
		// sid: 1, sname: joe
		// sid: 2, sname: amy
		t.Logf("sid: %d, sname: %s", sid, sname)
	}
}
