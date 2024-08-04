package query_test

import (
	"fmt"
	"math/rand"
	"path"
	"simpledb/query"
	"simpledb/record"
	"simpledb/server"
	"testing"
)

func TestSelectRecords(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "scantest1"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	schema1 := record.NewSchema()
	schema1.AddIntField("A")
	schema1.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(schema1)

	s1, err := query.NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	if err := s1.BeforeFirst(); err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	n := 200
	t.Logf("inserting %d random records", n)
	for i := 0; i < n; i++ {
		if err := s1.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		k := rand.Int31n(50)
		if err := s1.SetInt("A", k); err != nil {
			t.Fatalf("failed to set int field: %v", err)
		}
		if err := s1.SetString("B", fmt.Sprintf("rec%d", k)); err != nil {
			t.Fatalf("failed to set string field: %v", err)
		}
	}
	s1.Close()

	// selecting all records where A=10
	s2, err := query.NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	c := query.NewConstantWithInt(10)
	lhs := query.NewExpressionWithField("A")
	rhs := query.NewExpressionWithConstant(c)
	term := query.NewTerm(lhs, rhs)
	pred := query.NewPredicateWithTerm(term)
	t.Logf("the predicate is %v", pred)

	s3 := query.NewSelectScan(s2, pred)

	fields := []string{"B"}
	s4 := query.NewProjectScan(s3, fields)

	next, err := s4.Next()
	if err != nil {
		t.Fatalf("failed to call Next: %v", err)
	}
	for next {
		b, err := s4.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string field: %v", err)
		}

		t.Logf("B: %s", b)

		// A=10の条件で絞っているのでBはrec10のはず
		if b != "rec10" {
			t.Fatalf("expected rec10, got %s", b)
		}

		next, err = s4.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
	}
	s4.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}

func TestJoinAndSelectRecords(t *testing.T) {
	simpleDB, err := server.NewSimpleDBWithMetadata(path.Join(t.TempDir(), "scantest2"))
	if err != nil {
		t.Fatalf("failed to create simpledb: %v", err)
	}
	tx, err := simpleDB.NewTx()
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	// create T1
	schema1 := record.NewSchema()
	schema1.AddIntField("A")
	schema1.AddStringField("B", 9)
	layout1 := record.NewLayoutFromSchema(schema1)

	us1, err := query.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	if err := us1.BeforeFirst(); err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	n := 200
	t.Logf("inserting %d records into T1", n)
	for i := 0; i < n; i++ {
		if err := us1.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := us1.SetInt("A", int32(i)); err != nil {
			t.Fatalf("failed to set int field: %v", err)
		}
		if err := us1.SetString("B", fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("failed to set string field: %v", err)
		}
	}
	us1.Close()

	// create T2
	schema2 := record.NewSchema()
	schema2.AddIntField("C")
	schema2.AddStringField("D", 9)
	layout2 := record.NewLayoutFromSchema(schema2)

	us2, err := query.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}

	if err := us2.BeforeFirst(); err != nil {
		t.Fatalf("failed to call BeforeFirst: %v", err)
	}

	t.Logf("inserting %d records into T2", n)
	for i := 0; i < n; i++ {
		if err := us2.Insert(); err != nil {
			t.Fatalf("failed to insert record: %v", err)
		}
		if err := us2.SetInt("C", int32(n-i-1)); err != nil {
			t.Fatalf("failed to set int field: %v", err)
		}
		if err := us2.SetString("D", fmt.Sprintf("%d", n-i-1)); err != nil {
			t.Fatalf("failed to set string field: %v", err)
		}
	}
	us2.Close()

	// create a product scan
	s1, err := query.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	s2, err := query.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatalf("failed to create table scan: %v", err)
	}
	s3, err := query.NewProductScan(s1, s2)
	if err != nil {
		t.Fatalf("failed to create product scan: %v", err)
	}

	// selecting all records where A=C
	lhs := query.NewExpressionWithField("A")
	rhs := query.NewExpressionWithField("C")
	term := query.NewTerm(lhs, rhs)
	pred := query.NewPredicateWithTerm(term)
	t.Logf("the predicate is %v", pred)
	s4 := query.NewSelectScan(s3, pred)

	// projecting on [B, D]
	fields := []string{"B", "D"}
	s5 := query.NewProjectScan(s4, fields)

	next, err := s5.Next()
	if err != nil {
		t.Fatalf("failed to call Next: %v", err)
	}
	for next {
		b, err := s5.GetString("B")
		if err != nil {
			t.Fatalf("failed to get string field: %v", err)
		}
		d, err := s5.GetString("D")
		if err != nil {
			t.Fatalf("failed to get string field: %v", err)
		}
		t.Logf("B: %s, D: %s", b, d)

		// A=Cの条件で絞っているのでBとDは一致するはず
		if b != d {
			t.Fatalf("expected B==D, got B=%s, D=%s", b, d)
		}

		next, err = s5.Next()
		if err != nil {
			t.Fatalf("failed to call Next: %v", err)
		}
	}
	s5.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
}
