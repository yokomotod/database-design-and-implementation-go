package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/server"
	"simpledb/testlib"
	"testing"
)

func TestSortPlan(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "sort_plan_test"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	err = testlib.InsertSmallTestData(t, simpleDB)
	if err != nil {
		t.Fatalf("failed to setup test data: %v", err)
	}

	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	p, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	sortPlan, err := plan.NewSortPlan(tx, p, []string{"gradyear", "sname"})
	if err != nil {
		t.Fatalf("failed to create SortPlan: %v", err)
	}
	sortScan, err := sortPlan.Open()
	if err != nil {
		t.Fatalf("failed to open SortScan: %v", err)
	}

	err = sortScan.BeforeFirst()
	if err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	// 現在のレコードと次のレコードのgradyearとsnameを比較して、
	// レコードが昇順になっていることを確認する
	next, err := sortScan.Next()
	if err != nil {
		t.Fatalf("failed to get next: %v", err)
	}
	if !next {
		t.Fatalf("no record")
	}
	gradyear, err := sortScan.GetInt("gradyear")
	if err != nil {
		t.Fatalf("failed to get gradyear: %v", err)
	}
	sname, err := sortScan.GetString("sname")
	if err != nil {
		t.Fatalf("failed to get sname: %v", err)
	}
	for {
		t.Logf("gradyear: %d, sname: %s\n", gradyear, sname)
		next, err = sortScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !next {
			break
		}
		currentGradyear, err := sortScan.GetInt("gradyear")
		if err != nil {
			t.Fatalf("failed to get gradyear: %v", err)
		}
		currentSname, err := sortScan.GetString("sname")
		if err != nil {
			t.Fatalf("failed to get sname: %v", err)
		}

		if gradyear > currentGradyear {
			t.Fatalf("gradyear is not sorted: %d > %d", gradyear, currentGradyear)
		}
		if gradyear == currentGradyear && sname > currentSname {
			t.Fatalf("sname is not sorted: %s > %s", sname, currentSname)
		}

		gradyear = currentGradyear
		sname = currentSname
	}

	sortScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}
