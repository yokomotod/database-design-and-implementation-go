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

func TestMultibufferSortPlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "multibuffer_product_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = testlib.InsertLargeTestData(t, simpleDB)
	if err != nil {
		t.Fatalf("failed to setup test data: %v", err)
	}

	t.Run("MultibufferSortPlan", func(t *testing.T) {
		tx, err := simpleDB.NewTx()
		if err != nil {
			t.Fatalf("failed to create tx: %v", err)
		}

		p, err := plan.NewTablePlan(tx, "enroll", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of dept: %v", err)
		}

		sortPlan, err := plan.NewMultibufferSortPlan(tx, p, []string{"eid"})
		if err != nil {
			t.Fatalf("failed to create sort plan: %v", err)
		}

		tx.ResetBlockAccessed()
		got := executeSortPlan(t, tx, sortPlan)

		want := stats{
			PlanRecordsOutput:    100,
			PlanBlocksAccessed:   3,
			ActualRecordsOutput:  100,
			ActualBlocksAccessed: 34,
		}

		assert.Equal(t, want, got)
	})

	t.Run("SortPlan", func(t *testing.T) {
		tx, err := simpleDB.NewTx()
		if err != nil {
			t.Fatalf("failed to create tx: %v", err)
		}

		p, err := plan.NewTablePlan(tx, "enroll", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of dept: %v", err)
		}

		sortPlan, err := plan.NewSortPlan(tx, p, []string{"eid"})
		if err != nil {
			t.Fatalf("failed to create sort plan: %v", err)
		}

		tx.ResetBlockAccessed()
		got := executeSortPlan(t, tx, sortPlan)

		want := stats{
			PlanRecordsOutput:    100,
			PlanBlocksAccessed:   3,
			ActualRecordsOutput:  100,
			ActualBlocksAccessed: 596,
		}

		assert.Equal(t, want, got)
	})

}

func executeSortPlan(t *testing.T, tx *tx.Transaction, plan plan.Plan) stats {
	t.Helper()

	t.Logf("RecordsOutput: %d", plan.RecordsOutput())
	t.Logf("BlocksAccessed: %d", plan.BlocksAccessed())

	productScan, err := plan.Open()
	if err != nil {
		t.Fatalf("failed to open MergeJoinScan: %v", err)
	}
	err = productScan.BeforeFirst()
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
		next, err := productScan.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
		if !next {
			break
		}

		eid, err := productScan.GetInt("eid")
		if err != nil {
			t.Fatalf("failed to get dname: %v", err)
		}
		studentid, err := productScan.GetInt("studentid")
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

	productScan.Close()
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
