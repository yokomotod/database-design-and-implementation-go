package plan_test

import (
	"fmt"
	"path"
	"simpledb/plan"
	"simpledb/query"
	"simpledb/server"
	"simpledb/testlib"
	"testing"
)

func TestSingleTablePlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "single_table_plan_test"))
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

	p1, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	p2, err := plan.NewSelectPlan(p1, query.NewPredicateWithTerm(
		query.NewTerm(
			query.NewExpressionWithField("majorid"),
			query.NewExpressionWithConstant(
				query.NewConstantWithInt(10),
			),
		),
	))

	if err != nil {
		t.Fatalf("failed to create select plan p2: %v", err)
	}

	p3, err := plan.NewSelectPlan(p2, query.NewPredicateWithTerm(
		query.NewTerm(
			query.NewExpressionWithField("gradyear"),
			query.NewExpressionWithConstant(
				query.NewConstantWithInt(2020),
			),
		),
	))

	if err != nil {
		t.Fatalf("failed to create select plan p3: %v", err)
	}

	p4, err := plan.NewProjectPlan(p3, []string{"sname", "majorid", "gradyear"})
	if err != nil {
		t.Fatalf("failed to create project plan p4: %v", err)
	}

	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)

	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

}

func TestMultipleTablePlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "multiple_table_plan_test"))
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

	p1, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	p2, err := plan.NewTablePlan(tx, "dept", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	p3, err := plan.NewProductPlan(p1, p2)

	if err != nil {
		t.Fatalf("failed to create product plan p3: %v", err)
	}

	p4, err := plan.NewSelectPlan(p3, query.NewPredicateWithTerm(
		query.NewTerm(
			query.NewExpressionWithField("majorid"),
			query.NewExpressionWithField("did"),
		),
	))

	if err != nil {
		t.Fatalf("failed to create select plan p4: %v", err)
	}

	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)

	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

}

func printStats(n int, p plan.Plan) {
	planName := fmt.Sprintf("p%d", n)
	fmt.Println("Here are the stats for plan " + planName)
	fmt.Printf("\tR(%s): %d\n", planName, p.RecordsOutput())
	fmt.Printf("\tB(%s): %d\n", planName, p.BlocksAccessed())
	fmt.Println()
}
