package concurrency

import (
	"fmt"
	"sync"
	"time"

	"simpledb/file"
)

const maxLockTime = 10 * time.Second

var ErrTimeout = fmt.Errorf("timeout error")

type LockTable struct {
	locks map[file.BlockID]int
	cond  *sync.Cond
}

func newLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockID]int),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (l *LockTable) SLock(blockID file.BlockID) error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	startTime := time.Now()
	for {
		if time.Since(startTime) > maxLockTime {
			return ErrTimeout
		} else if !l.hasXLock(blockID) {
			break
		}
		l.waitWithTimeout(maxLockTime)
	}

	l.locks[blockID]++
	return nil
}

func (l *LockTable) XLock(blockID file.BlockID) error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	startTime := time.Now()
	for {
		if time.Since(startTime) > maxLockTime {
			return ErrTimeout
		} else if !l.hasOtherSLocks(blockID) {
			break
		}
		l.waitWithTimeout(maxLockTime)
	}

	l.locks[blockID] = -1
	return nil
}

func (l *LockTable) Unlock(blockID file.BlockID) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if l.locks[blockID] > 1 {
		l.locks[blockID]--
	} else {
		delete(l.locks, blockID)
		l.cond.Broadcast()
	}
}

func (l *LockTable) hasXLock(blockID file.BlockID) bool {
	return l.locks[blockID] < 0
}

func (l *LockTable) hasOtherSLocks(blockID file.BlockID) bool {
	return l.locks[blockID] > 1
}

// Java の `wait(MAX_TIME)` 相当を実現するために追加
func (l *LockTable) waitWithTimeout(timeout time.Duration) {
	timer := time.AfterFunc(timeout, func() {
		l.cond.L.Lock()
		defer l.cond.L.Unlock()
		l.cond.Broadcast()
	})
	l.cond.Wait()

	// NOTE: タイミング次第で `Stop()` 呼び出し前にすでに timer が動いてしまう可能性あり
	// その場合 Broadcast が実行されてしまうが、その際に Wait があってもなくても
	// 今の処理であればループでロック状態を確認しており特に問題はなさそうに見える
	timer.Stop()
}
