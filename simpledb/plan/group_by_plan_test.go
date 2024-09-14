package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/query"
	"simpledb/server"
	"simpledb/testlib"
	"testing"
)

func TestGroupByPlan(t *testing.T) {
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

	p, err := plan.NewTablePlan(tx, "student", simpleDB.MetadataManager())
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}

	// majorid でグルーピングし、gradyearの最大値・最小値を取得する
	groupByPlan, err := plan.NewGroupByPlan(
		tx,
		p,
		[]string{"majorid"},
		[]query.AggregationFn{
			query.NewMaxFn("gradyear"),
			query.NewMinFn("gradyear"),
			query.NewCountFn(""),
		},
	)
	if err != nil {
		t.Fatalf("failed to create GroupByPlan: %v", err)
	}
	groupByScan, err := groupByPlan.Open()
	if err != nil {
		t.Fatalf("failed to open GroupByScan: %v", err)
	}
	err = groupByScan.BeforeFirst()
	if err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	expects := map[int32]struct {
		minGradyear int32
		maxGradyear int32
		count       int32
	}{
		10: {2021, 2022, 4},
		20: {2019, 2022, 4},
		30: {2020, 2021, 2},
	}

	for {
		next, err := groupByScan.Next()
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if !next {
			break
		}
		majorid, err := groupByScan.GetInt("majorid")
		if err != nil {
			t.Fatalf("failed to get majorid: %v", err)
		}
		minGradyear, err := groupByScan.GetInt("min(gradyear)")
		if err != nil {
			t.Fatalf("failed to get min(majorid): %v", err)
		}
		maxGradyear, err := groupByScan.GetInt("max(gradyear)")
		if err != nil {
			t.Fatalf("failed to get max(majorid): %v", err)
		}
		count, err := groupByScan.GetInt("count()")
		if err != nil {
			t.Fatalf("failed to get count(): %v", err)
		}
		t.Logf("majorid: %d, minGradyear: %d, maxGradyear: %d, count: %d", majorid, minGradyear, maxGradyear, count)

		expect, ok := expects[majorid]
		if !ok {
			t.Errorf("unexpected majorid: %d", majorid)
		}
		if minGradyear != expect.minGradyear {
			t.Errorf("unexpected minGradyear of majorid %d: %d, expect: %d", majorid, minGradyear, expect.minGradyear)
		}
		if maxGradyear != expect.maxGradyear {
			t.Errorf("unexpected maxGradyear of majorid %d: %d, expect: %d", majorid, maxGradyear, expect.maxGradyear)
		}
		if count != expect.count {
			t.Errorf("unexpected count of majorid %d: %d, expect: %d", majorid, count, expect.count)
		}
	}

	groupByScan.Close()
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}
