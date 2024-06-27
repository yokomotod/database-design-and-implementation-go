package recovery

import (
	"fmt"

	"simpledb/file"
	"simpledb/log"
)

type LogRecordType int32

const (
	CheckPoint LogRecordType = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int32
	Undo(tx Transaction)
}

func NewLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageWith(bytes)
	switch LogRecordType(p.GetInt(0)) {
	case CheckPoint:
		return newCheckPointRecord(), nil
	case Start:
		return newStartRecordFrom(p), nil
	case Commit:
		return newCommitRecordFrom(p), nil
	case Rollback:
		return newRollbackRecordFrom(p), nil
	case SetInt:
		return newSetIntRecordFrom(p), nil
	case SetString:
		return newSetStringRecordFrom(p), nil
	default:
		return nil, fmt.Errorf("Unknown LogRecordType: %v", p.GetInt(0))
	}
}

type checkPointRecord struct{}

func newCheckPointRecord() *checkPointRecord {
	return &checkPointRecord{}
}

func (r *checkPointRecord) Op() LogRecordType {
	return CheckPoint
}

func (r *checkPointRecord) TxNumber() int32 {
	return 0
}

func (r *checkPointRecord) String() string {
	return "<CHECKPOINT>"
}

func (r *checkPointRecord) Undo(tx Transaction) {}

func (r *checkPointRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes

	buf := make([]byte, tpos)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(CheckPoint))
	return lm.Append(buf)
}

type startRecord struct {
	txnum int32
}

func newStartRecord(txnum int32) *startRecord {
	return &startRecord{
		txnum: txnum,
	}
}

func newStartRecordFrom(p *file.Page) *startRecord {
	return newStartRecord(p.GetInt(file.Int32Bytes))
}

func (r *startRecord) Op() LogRecordType {
	return Start
}

func (r *startRecord) TxNumber() int32 {
	return r.txnum
}

func (r *startRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txnum)
}

func (r *startRecord) Undo(tx Transaction) {}

func (r *startRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes

	reclen := tpos + file.Int32Bytes
	buf := make([]byte, reclen)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(Start))
	p.SetInt(tpos, r.txnum)
	return lm.Append(buf)
}

type commitRecord struct {
	txnum int32
}

func newCommitRecord(txnum int32) *commitRecord {
	return &commitRecord{
		txnum: txnum,
	}
}

func newCommitRecordFrom(p *file.Page) *commitRecord {
	return newCommitRecord(p.GetInt(file.Int32Bytes))
}

func (r *commitRecord) Op() LogRecordType {
	return Commit
}

func (r *commitRecord) TxNumber() int32 {
	return r.txnum
}

func (r *commitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txnum)
}

func (r *commitRecord) Undo(tx Transaction) {}

func (r *commitRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes

	reclen := tpos + file.Int32Bytes
	buf := make([]byte, reclen)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(Commit))
	p.SetInt(tpos, r.txnum)
	return lm.Append(buf)
}

type rollbackRecord struct {
	txnum int32
}

func newRollbackRecord(txnum int32) *rollbackRecord {
	return &rollbackRecord{
		txnum: txnum,
	}
}

func newRollbackRecordFrom(p *file.Page) *rollbackRecord {
	return newRollbackRecord(p.GetInt(file.Int32Bytes))
}

func (r *rollbackRecord) Op() LogRecordType {
	return Rollback
}

func (r *rollbackRecord) TxNumber() int32 {
	return r.txnum
}

func (r *rollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txnum)
}

func (r *rollbackRecord) Undo(tx Transaction) {}

func (r *rollbackRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes

	reclen := tpos + file.Int32Bytes
	buf := make([]byte, reclen)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(Rollback))
	p.SetInt(tpos, r.txnum)
	return lm.Append(buf)
}

type setIntRecord struct {
	txnum  int32
	offset int32
	val    int32
	blk    *file.BlockID
}

func newSetIntRecord(txnum int32, blk *file.BlockID, offset, val int32) *setIntRecord {
	return &setIntRecord{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func newSetIntRecordFrom(p *file.Page) *setIntRecord {
	tpos := file.Int32Bytes
	txNum := p.GetInt(tpos)

	fpos := tpos + file.Int32Bytes
	fileName := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(fileName))
	blkNum := p.GetInt(bpos)
	blk := file.NewBlockID(fileName, int64(blkNum))

	opos := bpos + file.Int32Bytes
	offset := p.GetInt(opos)

	vpos := opos + file.Int32Bytes
	val := p.GetInt(vpos)

	return newSetIntRecord(txNum, blk, offset, val)
}

func (r *setIntRecord) Op() LogRecordType {
	return SetInt
}

func (r *setIntRecord) TxNumber() int32 {
	return int32(r.txnum)
}

func (r *setIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", r.txnum, r.blk, r.offset, r.val)
}

func (r *setIntRecord) Undo(tx Transaction) {
	tx.Pin(r.blk)
	tx.SetInt(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)
}

func (r *setIntRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes
	fpos := tpos + file.Int32Bytes
	bpos := fpos + file.MaxLength(len(r.blk.FileName()))
	opos := bpos + file.Int32Bytes
	vpos := opos + file.Int32Bytes

	reclen := vpos + file.Int32Bytes
	buf := make([]byte, reclen)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(SetString))
	p.SetInt(tpos, r.txnum)
	p.SetString(fpos, r.blk.FileName())
	p.SetInt(bpos, int32(r.blk.Number()))
	p.SetInt(opos, r.offset)
	p.SetInt(vpos, r.val)

	return lm.Append(buf)
}

type setStringRecord struct {
	txnum  int32
	offset int32
	val    string
	blk    *file.BlockID
}

func newSetStringRecordFrom(p *file.Page) *setStringRecord {
	tpos := file.Int32Bytes
	txNum := p.GetInt(tpos)

	fpos := tpos + file.Int32Bytes
	fileName := p.GetString(fpos)
	bpos := fpos + file.MaxLength(len(fileName))
	blkNum := p.GetInt(bpos)
	blk := file.NewBlockID(fileName, int64(blkNum))

	opos := bpos + file.Int32Bytes
	offset := p.GetInt(opos)

	vpos := opos + file.Int32Bytes
	val := p.GetString(vpos)

	return &setStringRecord{
		txnum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (r *setStringRecord) Op() LogRecordType {
	return SetString
}

func (r *setStringRecord) TxNumber() int32 {
	return r.txnum
}

func (r *setStringRecord) Undo(tx Transaction) {
	tx.Pin(r.blk)
	tx.SetString(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)
}

func (r *setStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", r.txnum, r.blk, r.offset, r.val)
}

func (r *setStringRecord) WriteToLog(lm *log.Manager) (int, error) {
	tpos := file.Int32Bytes
	fpos := tpos + file.Int32Bytes
	bpos := fpos + file.MaxLength(len(r.blk.FileName()))
	opos := bpos + file.Int32Bytes
	vpos := opos + file.Int32Bytes

	reclen := vpos + file.MaxLength(len(r.val))
	buf := make([]byte, reclen)
	p := file.NewPageWith(buf)
	p.SetInt(0, int32(SetString))
	p.SetInt(tpos, r.txnum)
	p.SetString(fpos, r.blk.FileName())
	p.SetInt(bpos, int32(r.blk.Number()))
	p.SetInt(opos, r.offset)
	p.SetString(vpos, r.val)

	return lm.Append(buf)
}
