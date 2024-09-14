package plan_test

import (
	"path"
	"simpledb/plan"
	"simpledb/query"
	"simpledb/server"
	"simpledb/testlib"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexRetrieval(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "index_retrieval_test"))
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
	mdm := simpleDB.MetadataManager()

	studentPlan, err := plan.NewTablePlan(tx, "student", mdm)
	if err != nil {
		t.Fatalf("failed to create table plan: %v", err)
	}
	scan, err := studentPlan.Open()
	if err != nil {
		t.Fatalf("failed to open table scan: %v", err)
	}
	studentScan, ok := scan.(*query.TableScan)
	if !ok {
		t.Fatalf("scan is not a table scan")
	}

	// open the index on MajorId
	indexInfoMap, err := mdm.GetIndexInfo("student", tx)
	if err != nil {
		t.Fatalf("failed to get index info: %v", err)
	}
	majorIdIndexInfo, ok := indexInfoMap["majorid"]
	if !ok {
		t.Fatalf("no index on majorid")
	}
	indexInfo, err := majorIdIndexInfo.Open()
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}

	// retrieve all index records having a dataval of 20
	majorIdIndexVal := query.NewConstantWithInt(20)
	if err := indexInfo.BeforeFirst(majorIdIndexVal); err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}
	// NOTE: ここの loop には Ch.12 の時点では入らない
	//       insert 文で index に挿入する実装がまだないため
	for {
		ok, err := indexInfo.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
		if !ok {
			break
		}
		rid, err := indexInfo.GetDataRID()
		if err != nil {
			t.Fatalf("failed to get data rid: %v", err)
		}
		if err := studentScan.MoveToRID(rid); err != nil {
			t.Fatalf("failed to move to rid: %v", err)
		}
		studentName, err := studentScan.GetString("SName")
		if err != nil {
			t.Fatalf("failed to get student name: %v", err)
		}
		t.Logf("student name: %s", studentName)
	}

	indexInfo.Close()
	studentScan.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}

func TestIndexUpdate(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "index_update_test"))
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
	mdm := simpleDB.MetadataManager()
	studentPlan, err := plan.NewTablePlan(tx, "student", mdm)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := studentPlan.Open()
	if err != nil {
		t.Fatal(err)
	}
	studentScan, ok := plan.(*query.TableScan)
	if !ok {
		t.Fatal("scan is not a table scan")
	}

	indexInfoMap, err := mdm.GetIndexInfo("student", tx)
	if err != nil {
		t.Fatal(err)
	}
	indexes := make(map[string]query.Index)
	for fieldName, indexInfo := range indexInfoMap {
		idx, err := indexInfo.Open()
		if err != nil {
			t.Fatal(err)
		}
		indexes[fieldName] = idx
	}

	// Take 1: Insert a new Student record
	if err := studentScan.Insert(); err != nil {
		t.Fatal(err)
	}
	if err := studentScan.SetInt("sid", 11); err != nil {
		t.Fatal(err)
	}
	if err := studentScan.SetString("sname", "jim"); err != nil {
		t.Fatal(err)
	}
	if err := studentScan.SetInt("majorid", 10); err != nil {
		t.Fatal(err)
	}
	datarid, err := studentScan.GetRID()
	if err != nil {
		t.Fatal(err)
	}
	for fieldName, ii := range indexes {
		val, err := studentScan.GetVal(fieldName)
		if err != nil {
			t.Fatal(err)
		}
		if err := ii.Insert(val, datarid); err != nil {
			t.Fatal(err)
		}
	}

	// Take:3 check jim is inserted
	if err := studentScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	jimFound := false
	for {
		ok, err := studentScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		sname, err := studentScan.GetString("sname")
		if err != nil {
			t.Fatal(err)
		}
		if sname == "jim" {
			jimFound = true
			break
		}
	}
	assert.Truef(t, jimFound, "jim not found though he is inserted")

	// Take 2: Find and delete the new Student record
	if err := studentScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	for {
		ok, err := studentScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		sname, err := studentScan.GetString("sname")
		if err != nil {
			t.Fatal(err)
		}
		if sname == "jim" {
			rid, err := studentScan.GetRID()
			if err != nil {
				t.Fatal(err)
			}
			for fieldName, ii := range indexes {
				val, err := studentScan.GetVal(fieldName)
				if err != nil {
					t.Fatal(err)
				}
				if err := ii.Delete(val, rid); err != nil {
					t.Fatal(err)
				}
			}
			if err := studentScan.Delete(); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	// Print the records to verify the updates.
	if err := studentScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	for {
		ok, err := studentScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		sname, err := studentScan.GetString("sname")
		if err != nil {
			t.Fatal(err)
		}
		sid, err := studentScan.GetInt("sid")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("student name: %s, sid: %d", sname, sid)
		assert.NotEqualf(t, sname, "jim", "jim found tough the record id deleted")
		assert.NotEqual(t, sid, "11", "jim found tough the record id deleted")
	}
	studentScan.Close()
	for _, idx := range indexes {
		idx.Close()
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

}
