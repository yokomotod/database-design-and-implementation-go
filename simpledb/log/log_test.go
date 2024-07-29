package log_test

import (
	"fmt"
	"path"
	"strconv"
	"testing"

	"simpledb/file"
	"simpledb/log"
	"simpledb/server"
)

func TestLog(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "filetest"), 400, 8)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	logManager := db.LogManager()

	fmt.Println("The initial empty log file:")
	output := printLogRecords(logManager)
	if output != genWant(0) {
		t.Fatalf("got %v, want %v", output, genWant(0))
	}
	fmt.Println("done")

	createRecords(t, logManager, 1, 35)
	fmt.Println("The log file now has these records:")
	output = printLogRecords(logManager)
	if output != genWant(35) {
		t.Fatalf("got %v, want %v", output, genWant(35))
	}

	createRecords(t, logManager, 36, 70)
	logManager.Flush(65)
	fmt.Println("The log file now has these records:")
	output = printLogRecords(logManager)
	if output != genWant(70) {
		t.Fatalf("got %v, want %v", output, genWant(70))
	}
}

func printLogRecords(logManager *log.Manager) string {
	iter, err := logManager.Iterator()
	if err != nil {
		panic(err)
	}

	res := ""
	sentinel := 0
	for iter.HasNext() {
		rec, err := iter.Next()
		if err != nil {
			panic(err)
		}
		p := file.NewPageWith(rec)
		s := p.GetString(0)
		npos := file.MaxLength(int32(len(s)))
		val := p.GetInt(npos)
		output := fmt.Sprintf("[%s, %d]\n", s, val)
		fmt.Print(output)
		res += output

		sentinel++
		if sentinel > 100 {
			panic("Too many records")
		}
	}

	return res
}

func createRecords(t *testing.T, logManager *log.Manager, start, end int) {
	fmt.Println("Creating records:")

	for i := start; i <= end; i++ {
		rec := createLogRecord("record"+strconv.Itoa(i), i+100)
		lsn, err := logManager.Append(rec)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}

		fmt.Println(fmt.Sprint(lsn))
	}

	fmt.Println("")
}

// Create a log record having two values: a string and an integer.
func createLogRecord(s string, n int) []byte {
	var spos int32 = 0
	npos := spos + file.MaxLength(int32(len(s)))
	b := make([]byte, npos+file.Int32Bytes)
	p := file.NewPageWith(b)
	p.SetString(spos, s)
	p.SetInt(npos, int32(n))
	return b
}

func genWant(n int) string {
	want := ""
	for i := n; i > 0; i-- {
		want += fmt.Sprintf("[%s, %d]\n", "record"+strconv.Itoa(i), i+100)
	}
	return want
}
