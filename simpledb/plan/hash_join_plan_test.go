package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/server"
	"simpledb/testlib"
	"simpledb/tx"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashJoinPlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "hash_join_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = testlib.InsertLargeTestData(t, simpleDB)
	if err != nil {
		t.Fatalf("failed to setup test data: %v", err)
	}

	t.Run("HashJoinPlan", func(t *testing.T) {
		tx, err := simpleDB.NewTx()
		if err != nil {
			t.Fatalf("failed to create tx: %v", err)
		}

		p1, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of student: %v", err)
		}
		p2, err := plan.NewTablePlan(tx, "enroll", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of dept: %v", err)
		}

		hashJoinPlan, err := plan.NewHashJoinPlan(tx, p1, p2, "sid", "studentid")
		if err != nil {
			t.Fatalf("failed to create hash join plan: %v", err)
		}

		tx.ResetBlockAccessed()
		got := executeJoinPlan(t, tx, hashJoinPlan)

		want := stats{
			PlanRecordsOutput:    -1,
			PlanBlocksAccessed:   -1,
			ActualRecordsOutput:  1000,
			ActualBlocksAccessed: 18,
		}

		assert.Equal(t, want, got)
	})

}

func executeJoinPlan(t *testing.T, tx *tx.Transaction, plan plan.Plan) stats {
	t.Helper()

	t.Logf("RecordsOutput: %d", plan.RecordsOutput())
	t.Logf("BlocksAccessed: %d", plan.BlocksAccessed())

	joinScan, err := plan.Open()
	if err != nil {
		t.Fatalf("failed to open MergeJoinScan: %v", err)
	}
	err = joinScan.BeforeFirst()
	if err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	// depts := []string{
	// 	"compsci",
	// 	"math",
	// 	"drama",
	// }

	// students := []string{
	// 	"joe",
	// 	"amy",
	// 	"max",
	// 	"sue",
	// 	"bob",
	// 	"kim",
	// 	"art",
	// 	"pat",
	// 	"lee",
	// 	"dan",
	// }

	type record struct {
		Eid       int32
		Studentid int32
	}

	// expects := make([]record, 0, len(depts)*len(students))
	// for _, dname := range depts {
	// 	for _, sname := range students {
	// 		expects = append(expects, record{dname, sname})
	// 	}
	// }

	got := make([]record, 0)
	for i := 0; ; i++ {
		next, err := joinScan.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
		if !next {
			break
		}

		eid, err := joinScan.GetInt("eid")
		if err != nil {
			t.Fatalf("failed to get dname: %v", err)
		}
		studentid, err := joinScan.GetInt("studentid")
		if err != nil {
			t.Fatalf("failed to get sname: %v", err)
		}
		t.Logf("eid: %d, studentid: %d", eid, studentid)

		got = append(got, record{eid, studentid})
	}

	// assert.Eq
	// if diff := cmp.Diff(expects, got); diff != "" {
	// 	t.Errorf("unexpected result, diff: %v", diff)
	// }

	joinScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

	return stats{
		PlanRecordsOutput:    plan.RecordsOutput(),
		PlanBlocksAccessed:   plan.BlocksAccessed(),
		ActualRecordsOutput:  len(got),
		ActualBlocksAccessed: tx.BlockAccessed(),
	}
}
