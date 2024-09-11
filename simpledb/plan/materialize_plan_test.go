package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/query"
	"simpledb/server"
	"simpledb/testlib"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestMaterializePlan(t *testing.T) {
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
	s2, err := plan.NewSelectPlan(p2, query.NewPredicateWithTerm(
		query.NewTerm(
			query.NewExpressionWithField("gradyear"),
			query.NewExpressionWithConstant(query.NewConstantWithInt(2020)),
		),
	))
	if err != nil {
		t.Fatalf("failed to create select plan s1: %v", err)
	}
	m2 := plan.NewMaterializePlan(tx, s2)

	productPlan, err := plan.NewProductPlan(p1, m2)
	if err != nil {
		t.Fatalf("failed to create MergeJoinPlan: %v", err)
	}

	t.Logf("RecordsOutput: %d", productPlan.RecordsOutput())
	t.Logf("BlocksAccessed: %d", productPlan.BlocksAccessed())
	assert.Equal(t, int32(6), productPlan.RecordsOutput())  // 3 depts * (10 / (1 + 10/3)) students = 3 * 2 = 6
	assert.Equal(t, int32(4), productPlan.BlocksAccessed()) // 3 depts

	productScan, err := productPlan.Open()
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
		"amy",
		"bob",
		"dan",
	}

	type record struct {
		DName   string
		SName   string
		Gradear int32
	}

	expects := make([]record, 0, len(depts)*len(students))
	for _, dname := range depts {
		for _, sname := range students {
			expects = append(expects, record{dname, sname, 2020})
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
		gradyear, err := productScan.GetInt("gradyear")
		if err != nil {
			t.Fatalf("failed to get gradyear: %v", err)
		}
		t.Logf("dname: %s, sname: %s, gradyear: %d", dname, sname, gradyear)

		got = append(got, record{dname, sname, gradyear})
	}

	if diff := cmp.Diff(expects, got); diff != "" {
		t.Errorf("unexpected result, diff: %v", diff)
	}

	productScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}
