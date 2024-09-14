package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/server"
	"simpledb/testlib"
	"testing"
)

func TestMergeJoinPlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "sort_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = testlib.InsertTestData(t, simpleDB)
	if err != nil {
		t.Fatalf("failed to setup test data: %v", err)
	}

	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	p1, err := plan.NewTablePlan(tx, "dept", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan of dept: %v", err)
	}

	p2, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan of student: %v", err)
	}

	// dept と student を join して、生徒名と学部名の組み合わせが取得できることを確認する
	mergeJoinPlan, err := plan.NewMergeJoinPlan(tx, p1, p2, "did", "majorid")
	if err != nil {
		t.Fatalf("failed to create MergeJoinPlan: %v", err)
	}
	mergeJoinScan, err := mergeJoinPlan.Open()
	if err != nil {
		t.Fatalf("failed to open MergeJoinScan: %v", err)
	}
	err = mergeJoinScan.BeforeFirst()
	if err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	expects := map[string]string{
		"joe": "compsci",
		"amy": "math",
		"max": "compsci",
		"sue": "math",
		"bob": "drama",
		"kim": "math",
		"art": "drama",
		"pat": "compsci",
		"lee": "compsci",
		"dan": "math",
	}

	for {
		next, err := mergeJoinScan.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
		if !next {
			break
		}

		sname, err := mergeJoinScan.GetString("sname")
		if err != nil {
			t.Fatalf("failed to get sname: %v", err)
		}
		dname, err := mergeJoinScan.GetString("dname")
		if err != nil {
			t.Fatalf("failed to get dname: %v", err)
		}
		t.Logf("sname: %s, dname: %s", sname, dname)

		expect, ok := expects[sname]
		if !ok {
			t.Errorf("unexpected sname: %s", sname)
		}
		if dname != expect {
			t.Errorf("unexpected dname of sname %s: %s, expect: %s", sname, dname, expect)
		}
	}

	mergeJoinScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}
