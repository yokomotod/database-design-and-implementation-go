package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const indexInfoFieldBlock = "block"
const indexInfoFieldId = "id"
const indexInfoFieldDataVal = "dataval"

const indexCatalogTableName = "idxcat"
const indexCatalogFieldIndexName = "indexname"
const indexCatalogFieldTableName = "tablename"
const indexCatalogFieldFieldName = "fieldname"

type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *tx.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	si          *StatInfo
}

func NewIndexInfo(indexName string, fieldName string, tableSchema *record.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {
	ii := &IndexInfo{indexName, fieldName, tx, tableSchema, nil, si}
	ii.indexLayout = ii.createIndexLayout()
	return ii
}

// TODO: HashIndex が作られた際に要再実装
// func (ii *IndexInfo) Open() *Index {
// 	return NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
// }
//
// func (ii *IndexInfo) BlocksAccessed() int {
// 	rpb := ii.tx.BlockSize() / ii.indexLayout.SlotSize()
// 	numblocks := ii.si.RecordsOutput() / rpb
// 	return HashIndex.SearchCost(numblocks, rpb)
// }

func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) DistinctValues(fname string) int {
	var result int
	if ii.fieldName == fname {
		result = 1
	} else {
		result = ii.si.DistinctValues(ii.fieldName)
	}
	return result
}

func (ii *IndexInfo) createIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField(indexInfoFieldBlock)
	schema.AddIntField(indexInfoFieldId)
	if ii.tableSchema.Type(ii.fieldName) == record.INT {
		schema.AddIntField(indexInfoFieldDataVal)
	} else {
		fldlen := ii.tableSchema.Length(ii.fieldName)
		schema.AddStringField(indexInfoFieldDataVal, fldlen)
	}
	return record.NewLayoutFromSchema(schema)
}

type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *tx.Transaction) (*IndexManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(indexCatalogFieldIndexName, MaxName)
		schema.AddStringField(indexCatalogFieldTableName, MaxName)
		schema.AddStringField(indexCatalogFieldFieldName, MaxName)
		err := tableManager.CreateTable(indexCatalogTableName, schema, tx)
		if err != nil {
			return nil, err
		}
	}
	layout, err := tableManager.GetLayout(indexCatalogTableName, tx)
	if err != nil {
		return nil, err
	}
	return &IndexManager{layout, tableManager, statManager}, nil
}

func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) error {
	ts, err := record.NewTableScan(tx, indexCatalogTableName, im.layout)
	if err != nil {
		return err
	}
	defer ts.Close()

	ts.Insert()
	ts.SetString(indexCatalogFieldIndexName, indexName)
	ts.SetString(indexCatalogFieldTableName, tableName)
	ts.SetString(indexCatalogFieldFieldName, fieldName)
	return nil
}

func (im *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) (map[string]*IndexInfo, error) {
	result := make(map[string]*IndexInfo)
	ts, err := record.NewTableScan(tx, indexCatalogTableName, im.layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()
	next, err := ts.Next()
	if err != nil {
		return nil, err
	}
	for next {
		tn, err := ts.GetString(indexCatalogFieldTableName)
		if err != nil {
			return nil, err
		}
		if tn == tableName {
			indexName, err := ts.GetString(indexCatalogFieldIndexName)
			if err != nil {
				return nil, err
			}
			fieldName, err := ts.GetString(indexCatalogFieldFieldName)
			if err != nil {
				return nil, err
			}
			tblLayout, err := im.tableManager.GetLayout(tableName, tx)
			if err != nil {
				return nil, err
			}
			tblsi, err := im.statManager.GetStatInfo(tableName, tblLayout, tx)
			if err != nil {
				return nil, err
			}
			ii := NewIndexInfo(indexName, fieldName, tblLayout.Schema(), tx, tblsi)
			result[fieldName] = ii
		}
		next, err = ts.Next()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
