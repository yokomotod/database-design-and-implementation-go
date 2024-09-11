package metadata

import (
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
	"sync"
)

type StatInfo struct {
	numBlocks int32
	numRecs   int32
}

func NewStatInfo(numBlocks, numRecs int32) *StatInfo {
	return &StatInfo{numBlocks, numRecs}
}

func (si *StatInfo) BlocksAccessed() int32 {
	return si.numBlocks
}

func (si *StatInfo) RecordsOutput() int32 {
	return si.numRecs
}

func (si *StatInfo) DistinctValues(fieldName string) int32 {
	// fmt.Printf("si.DistinctValues(%q) = %d\n", fieldName, 1+(si.numRecs/3))
	return 1 + (si.numRecs / 3) // This is wildly inaccurate.
}

type StatManager struct {
	logger *logger.Logger

	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int
	mux          *sync.Mutex
}

func NewStatManager(tableManager *TableManager, tx *tx.Transaction) (*StatManager, error) {
	logger := logger.New("metadata.StatManager", logger.Trace)

	statManager := &StatManager{logger, tableManager, nil, 0, &sync.Mutex{}}
	err := statManager.refreshStatistics(tx)
	if err != nil {
		return nil, err
	}
	return statManager, nil
}

func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	// 書籍では refreshStatistics, calcTableStats にも synchronized がついているが
	// golang でそのまま全てのメソッドでロックを取るとデッドロックになってしまう。
	// 幸い両メソッドはプライベートであり、コンストラクタと本メソッド以外から呼び
	// 出される心配はないのでロックは取らない。
	sm.mux.Lock()
	defer sm.mux.Unlock()

	sm.logger.Tracef("GetStatInfo(%q)", tableName)

	sm.numCalls++
	if sm.numCalls > 100 {
		err := sm.refreshStatistics(tx)
		if err != nil {
			return nil, err
		}
	}
	si := sm.tableStats[tableName]
	if si == nil {
		si, err := sm.calcTableStats(tableName, layout, tx)
		if err != nil {
			return nil, err
		}
		sm.tableStats[tableName] = si
	}
	return sm.tableStats[tableName], nil
}

func (sm *StatManager) refreshStatistics(tx *tx.Transaction) error {
	sm.logger.Tracef("refreshStatistics()")

	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0
	tableCatalogLayout, err := sm.tableManager.GetLayout(tableCatalogTableName, tx)
	if err != nil {
		return err
	}
	tableCatalog, err := query.NewTableScan(tx, tableCatalogTableName, tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalog.Close()

	for {
		next, err := tableCatalog.Next()
		if err != nil {
			return err
		}
		if !next {
			break
		}
		tableName, err := tableCatalog.GetString(tableCatalogFieldTableName)
		if err != nil {
			return err
		}
		layout, err := sm.tableManager.GetLayout(tableName, tx)
		if err != nil {
			return err
		}
		si, err := sm.calcTableStats(tableName, layout, tx)
		if err != nil {
			return err
		}
		sm.tableStats[tableName] = si
	}
	return nil
}

func (sm *StatManager) calcTableStats(tableName string, layout *record.Layout, tx *tx.Transaction) (*StatInfo, error) {
	sm.logger.Tracef("calcTableStats(%q)", tableName)

	numRecs := int32(0)
	numBlocks := int32(0)
	ts, err := query.NewTableScan(tx, tableName, layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	for {
		next, err := ts.Next()
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}

		numRecs++
		rid, err := ts.GetRID()
		if err != nil {
			return nil, err
		}
		numBlocks = rid.BlockNumber() + 1
	}
	sm.logger.Debugf("calcTableStats(%q): numRecs=%d, numBlocks=%d", tableName, numRecs, numBlocks)
	return NewStatInfo(numBlocks, numRecs), nil
}
