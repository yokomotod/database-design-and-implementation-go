package query

import (
	"simpledb/tx"
	"simpledb/util/logger"
)

var _ Scan = (*HashJoinScan)(nil)

type PlanLike interface {
	Open() (Scan, error)
	BeforeFirst() error
}

type HashJoinScan struct {
	logger *logger.Logger

	tx                 *tx.Transaction
	buckets1, buckets2 []*TempTable
	currentBucket      int
	currentScan        *MultibufferProductScan
}

func NewHashJoinScan(tx *tx.Transaction, buckets1, buckets2 []*TempTable) *HashJoinScan {
	return &HashJoinScan{
		logger: logger.New("query.HashJoinScan", logger.Trace),

		tx:       tx,
		buckets1: buckets1,
		buckets2: buckets2,
	}
}

func (hjs *HashJoinScan) BeforeFirst() error {
	hjs.currentBucket = 0

	leftscan, err := hjs.buckets1[hjs.currentBucket].Open()
	if err != nil {
		return err
	}

	hjs.currentScan, err = NewMultibufferProductScan(hjs.tx, leftscan, hjs.buckets2[hjs.currentBucket].TableName, hjs.buckets2[hjs.currentBucket].Layout())
	if err != nil {
		return err
	}

	err = hjs.currentScan.BeforeFirst()
	if err != nil {
		return err
	}

	return nil
}

func (hjs *HashJoinScan) Next() (bool, error) {
	next, err := hjs.currentScan.Next()
	if err != nil {
		return false, err
	}

	if !next {
		hjs.logger.Tracef("Next(): next bucket ?: currentBucket=%d, len(buckets1)=%d", hjs.currentBucket, len(hjs.buckets1))

		hjs.currentBucket++

		if hjs.currentBucket >= len(hjs.buckets1) {
			return false, nil
		}

		hjs.currentScan.Close()
		leftscan, err := hjs.buckets1[hjs.currentBucket].Open()
		if err != nil {
			return false, err
		}

		hjs.currentScan, err = NewMultibufferProductScan(hjs.tx, leftscan, hjs.buckets2[hjs.currentBucket].TableName, hjs.buckets2[hjs.currentBucket].Layout())
		if err != nil {
			return false, err
		}

		err = hjs.currentScan.BeforeFirst()
		if err != nil {
			return false, err
		}

		return hjs.Next()
	}

	return true, nil
}

func (hjs *HashJoinScan) Close() {
	hjs.currentScan.Close()
}

func (hjs *HashJoinScan) GetInt(fieldName string) (int32, error) {
	return hjs.currentScan.GetInt(fieldName)
}

func (hjs *HashJoinScan) GetString(fieldName string) (string, error) {
	return hjs.currentScan.GetString(fieldName)
}

func (hjs *HashJoinScan) GetVal(fieldName string) (*Constant, error) {
	return hjs.currentScan.GetVal(fieldName)
}

func (hjs *HashJoinScan) HasField(fieldName string) bool {
	return hjs.currentScan.HasField(fieldName)
}
