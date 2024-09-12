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

type stats struct {
	PlanRecordsOutput    int32
	PlanBlocksAccessed   int32
	ActualRecordsOutput  int
	ActualBlocksAccessed int
}

func TestNewMultibufferProductPlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "multibuffer_product_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = testlib.InsertMiddleTestData(t, simpleDB)
	if err != nil {
		t.Fatalf("failed to setup test data: %v", err)
	}

	t.Run("MultibufferProductPlan", func(t *testing.T) {
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

		productPlan := plan.NewMultibufferProductPlan(tx, p1, p2)

		tx.ResetBlockAccessed()
		got := executePlan(t, tx, productPlan)

		want := stats{
			PlanRecordsOutput:    1000, // 10 students * 100 depts = 1000
			PlanBlocksAccessed:   4,
			ActualRecordsOutput:  1000,
			ActualBlocksAccessed: 18,
		}

		assert.Equal(t, want, got)
	})

	t.Run("ProductPlan", func(t *testing.T) {
		tx, err := simpleDB.NewTx()
		if err != nil {
			t.Fatalf("failed to create tx: %v", err)
		}
		t.Logf("initial BlockAccessed: %d", tx.BlockAccessed())

		p1, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of student: %v", err)
		}
		p2, err := plan.NewTablePlan(tx, "enroll", simpleDB.MetadataManager())
		if err != nil {
			t.Fatalf("failed to create table plan of dept: %v", err)
		}

		productPlan, err := plan.NewProductPlan(p1, p2)
		if err != nil {
			t.Fatalf("failed to create product plan: %v", err)
		}

		tx.ResetBlockAccessed()
		got := executePlan(t, tx, productPlan)

		want := stats{
			PlanRecordsOutput:    1000, // 10 students * 100 depts = 1000
			PlanBlocksAccessed:   41,
			ActualRecordsOutput:  1000,
			ActualBlocksAccessed: 42,
		}

		assert.Equal(t, want, got)
	})
}

func executePlan(t *testing.T, tx *tx.Transaction, plan plan.Plan) stats {
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

	depts := []string{
		"compsci",
		"math",
		"drama",
	}

	students := []string{
		"joe",
		"amy",
		"max",
		"sue",
		"bob",
		"kim",
		"art",
		"pat",
		"lee",
		"dan",
	}

	type record struct {
		DName string
		SName string
	}

	expects := make([]record, 0, len(depts)*len(students))
	for _, dname := range depts {
		for _, sname := range students {
			expects = append(expects, record{dname, sname})
		}
	}

	got := make([]record, 0)
	for i := 0; ; i++ {
		next, err := productScan.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
		if !next {
			break
		}

		dname, err := productScan.GetString("dname")
		if err != nil {
			t.Fatalf("failed to get dname: %v", err)
		}
		sname, err := productScan.GetString("sname")
		if err != nil {
			t.Fatalf("failed to get sname: %v", err)
		}
		// t.Logf("dname: %s, sname: %s", dname, sname)

		got = append(got, record{dname, sname})
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
