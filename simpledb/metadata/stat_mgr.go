package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

type StatInfo struct {
	numBlocks int
	numRecs   int
}

func NewStatInfo(numblocks int, numrecs int) *StatInfo {
	return &StatInfo{numblocks, numrecs}
}

func (si *StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

func (si *StatInfo) RecordsOutput() int {
	return si.numRecs
}

func (si *StatInfo) DistinctValues(fldname string) int {
	return 1 + (si.numRecs / 3) // This is wildly inaccurate.
}

type StatMgr struct {
	tblMgr     *TableMgr
	tablestats map[string]*StatInfo
	numcalls   int
}

func NewStatMgr(tblMgr *TableMgr, tx *tx.Transaction) (*StatMgr, error) {
	statMgr := &StatMgr{tblMgr, nil, 0}
	err := statMgr.refreshStatistics(tx)
	if err != nil {
		return nil, err
	}
	return statMgr, nil
}

func (sm *StatMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	sm.numcalls++
	if sm.numcalls > 100 {
		err := sm.refreshStatistics(tx)
		if err != nil {
			return nil, err
		}
	}
	si := sm.tablestats[tblname]
	if si == nil {
		si, err := sm.calcTableStats(tblname, layout, tx)
		if err != nil {
			return nil, err
		}
		sm.tablestats[tblname] = si
	}
	return sm.tablestats[tblname], nil
}

func (sm *StatMgr) refreshStatistics(tx *tx.Transaction) error {
	sm.tablestats = make(map[string]*StatInfo)
	sm.numcalls = 0
	tcatLayout, err := sm.tblMgr.GetLayout("tblcat", tx)
	if err != nil {
		return err
	}
	tcat, err := record.NewTableScan(tx, "tblcat", tcatLayout)
	if err != nil {
		return err
	}
	next, err := tcat.Next()
	if err != nil {
		return err
	}
	for next {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return err
		}
		layout, err := sm.tblMgr.GetLayout(tblname, tx)
		if err != nil {
			return err
		}
		si, err := sm.calcTableStats(tblname, layout, tx)
		if err != nil {
			return err
		}
		sm.tablestats[tblname] = si
		next, err = tcat.Next()
		if err != nil {
			return err
		}
	}
	tcat.Close()
	return nil
}

func (sm *StatMgr) calcTableStats(tblname string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	numRecs := 0
	numblocks := 0
	ts, err := record.NewTableScan(tx, tblname, layout)
	if err != nil {
		return nil, err
	}
	next, err := ts.Next()
	if err != nil {
		return nil, err
	}
	for next {
		numRecs++
		numblocks = int(ts.GetRID().BlockNumber()) + 1
		next, err = ts.Next()
		if err != nil {
			return nil, err
		}
	}
	ts.Close()
	return NewStatInfo(numblocks, numRecs), nil
}