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
	err = testlib.InsertMiddleTestData(t, simpleDB)
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

		tx.ResetBlocksAccessed()
		got := executeJoinPlan(t, tx, hashJoinPlan)

		want := stats{
			PlanRecordsOutput:    -1,
			PlanBlocksAccessed:   -1,
			ActualRecordsOutput:  100,
			ActualBlocksAccessed: 30,
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

	type record struct {
		Eid       int32
		Studentid int32
	}

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
			t.Fatalf("failed to get eid: %v", err)
		}
		sid, err := joinScan.GetInt("sid")
		if err != nil {
			t.Fatalf("failed to get sid: %v", err)
		}
		studentid, err := joinScan.GetInt("studentid")
		if err != nil {
			t.Fatalf("failed to get sname: %v", err)
		}
		t.Logf("eid: %d, sid: %d, studentid: %d", eid, sid, studentid)
		if sid != studentid {
			t.Fatalf("sid and studentid are not equal: sid=%d, studentid=%d", sid, studentid)
		}

		got = append(got, record{eid, studentid})
	}

	joinScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

	return stats{
		PlanRecordsOutput:    plan.RecordsOutput(),
		PlanBlocksAccessed:   plan.BlocksAccessed(),
		ActualRecordsOutput:  len(got),
		ActualBlocksAccessed: tx.BlocksAccessed(),
	}
}
