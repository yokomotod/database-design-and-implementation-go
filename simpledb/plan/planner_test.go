package plan_test

import (
	"path"
	"simpledb/server"
	"testing"
)

func TestPlannerStudent(t *testing.T) {
	cases := []struct {
		name     string
		useBasic bool
	}{
		{"Basic", true},
		{"Indexed", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var simpleDB *server.SimpleDB
			var err error
			if c.useBasic {
				simpleDB, err = server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "studentdb"))
			} else {
				simpleDB, err = server.NewIndexedSimpleDB(path.Join(t.TempDir(), "studentdb"))
			}
			if err != nil {
				t.Fatalf("failed to create simpledb: %v", err)
			}
			tx, err := simpleDB.NewTx()
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}

			planner := simpleDB.Planner()

			_, err = planner.ExecuteUpdate("create table student (sid int, sname varchar(10), majorid int, gradyear int)", tx)
			if err != nil {
				t.Fatalf("failed to create table: %v", err)
			}
			t.Logf("created table student")

			cnt, err := planner.ExecuteUpdate("insert into student(sid, sname, majorid) values (1, 'joe', 10, 2021)", tx)
			if err != nil {
				t.Fatalf("failed to insert into table: %v", err)
			}
			t.Logf("insert into student: %d", cnt)
			if cnt != 1 {
				t.Errorf("inserted count: want: 1, got: %d", cnt)
			}

			cnt, err = planner.ExecuteUpdate("insert into student(sid, sname, majorid) values (2, 'xxx', 20, 2020)", tx)
			if err != nil {
				t.Fatalf("failed to insert into table: %v", err)
			}
			t.Logf("insert into student: %d", cnt)
			if cnt != 1 {
				t.Errorf("inserted count: want: 1, got: %d", cnt)
			}

			cnt, err = planner.ExecuteUpdate("update student set sname = 'amy' where sid = 2", tx)
			if err != nil {
				t.Fatalf("failed to update table: %v", err)
			}
			if cnt != 1 {
				t.Errorf("updated count: want: 1, got: %d", cnt)
			}

			// this is to be deleted by the following statements
			_, err = planner.ExecuteUpdate("insert into student(sid, sname, majorid) values (3, 'tbd', 20, 2020)", tx)
			if err != nil {
				t.Fatalf("failed to insert into table: %v", err)
			}

			cnt, err = planner.ExecuteUpdate("delete from student where sid = 99", tx)
			if err != nil {
				t.Fatalf("failed to delete from table: %v", err)
			}
			if cnt != 0 {
				t.Errorf("deleted count: want: 0, got: %d", cnt)
			}

			cnt, err = planner.ExecuteUpdate("delete from student where sid = 3", tx)
			if err != nil {
				t.Fatalf("failed to delete from table: %v", err)
			}
			if cnt != 1 {
				t.Errorf("deleted count: want: 1, got: %d", cnt)
			}

			plan, err := planner.CreateQueryPlan("select sid, sname from student", tx)
			if err != nil {
				t.Fatalf("failed to create query plan: %v", err)
			}

			sc, err := plan.Open()
			if err != nil {
				t.Fatalf("failed to open scan: %v", err)
			}

			type student struct {
				sid   int32
				sname string
			}
			want := []student{
				{1, "joe"},
				{2, "amy"},
			}
			got := make([]student, 0, 2)

			for {
				next, err := sc.Next()
				if err != nil {
					t.Fatalf("failed to get next: %v", err)
				}
				if !next {
					break
				}

				sid, err := sc.GetInt("sid")
				if err != nil {
					t.Fatalf("failed to get int: %v", err)
				}
				sname, err := sc.GetString("sname")
				if err != nil {
					t.Fatalf("failed to get string: %v", err)
				}
				got = append(got, student{sid, sname})
				t.Logf("sid: %d, sname: %s", sid, sname)
			}

			for i, s := range got {
				if s != want[i] {
					t.Errorf("want: %v, got: %v", want[i], got[i])
				}
			}
			sc.Close()
			err = tx.Commit()
			if err != nil {
				t.Fatalf("failed to commit: %v", err)
			}
		})
	}
}
