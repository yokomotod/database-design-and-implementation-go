package plan_test

import (
	"fmt"
	"path"
	"simpledb/plan"
	"simpledb/query"
	"simpledb/server"
	"testing"
)

func TestSingleTablePlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "single_table_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = insertTestData(simpleDB)
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
	err = insertTestData(simpleDB)
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

func insertTestData(simpledb *server.SimpleDB) error {
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
		{6, "kim", 20, 2020},
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
