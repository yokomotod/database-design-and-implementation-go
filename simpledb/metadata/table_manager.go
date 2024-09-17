package metadata

import (
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"simpledb/util/logger"
)

const MaxName = 16
const tableCatalogTableName = "tblcat"
const tableCatalogFieldTableName = "tblname"
const tableCatalogFieldSlotSize = "slotsize"
const fieldCatalogTableName = "fldcat"
const fieldCatalogFieldTableName = "tblname"
const fieldCatalogFieldFieldName = "fldname"
const fieldCatalogFieldType = "type"
const fieldCatalogFieldLength = "length"
const fieldCatalogFieldOffset = "offset"

type TableManager struct {
	logger *logger.Logger

	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) (*TableManager, error) {
	logger := logger.New("metadata.TableManager", logger.Trace)

	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStringField(tableCatalogFieldTableName, MaxName)
	tableCatalogSchema.AddIntField(tableCatalogFieldSlotSize)
	tableCatalogLayout := record.NewLayoutFromSchema(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStringField(fieldCatalogFieldTableName, MaxName)
	fieldCatalogSchema.AddStringField(fieldCatalogFieldFieldName, MaxName)
	fieldCatalogSchema.AddIntField(fieldCatalogFieldType)
	fieldCatalogSchema.AddIntField(fieldCatalogFieldLength)
	fieldCatalogSchema.AddIntField(fieldCatalogFieldOffset)
	fieldCatalogLayout := record.NewLayoutFromSchema(fieldCatalogSchema)

	tableManager := &TableManager{logger, tableCatalogLayout, fieldCatalogLayout}
	if isNew {
		logger.Tracef("(%q) NewTableManager(): CreateTable", tableCatalogTableName)
		if err := tableManager.CreateTable(tableCatalogTableName, tableCatalogSchema, tx); err != nil {
			return nil, err
		}
		logger.Tracef("(%q) NewTableManager(): CreateTable", fieldCatalogTableName)
		if err := tableManager.CreateTable(fieldCatalogTableName, fieldCatalogSchema, tx); err != nil {
			return nil, err
		}
	}
	return tableManager, nil
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	tm.logger.Tracef("(%q) CreateTable", tableName)

	layout := record.NewLayoutFromSchema(schema)
	tableCatalog, err := query.NewTableScan(tx, tableCatalogTableName, tm.tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalog.Close()

	tm.logger.Tracef("(%q) CreateTable(): tableCatalog.Insert", tableName)
	if err := tableCatalog.Insert(); err != nil {
		return err
	}
	if err := tableCatalog.SetString(tableCatalogFieldTableName, tableName); err != nil {
		return err
	}
	if err := tableCatalog.SetInt(tableCatalogFieldSlotSize, layout.SlotSize()); err != nil {
		return err
	}

	fieldCatalog, err := query.NewTableScan(tx, fieldCatalogTableName, tm.fieldCatalogLayout)
	if err != nil {
		return err
	}
	defer fieldCatalog.Close()

	for _, fieldName := range schema.Fields() {
		tm.logger.Tracef("(%q) CreateTable(): fieldCatalog.Insert(): `%s.%s`", tableName, tableName, fieldName)
		if err := fieldCatalog.Insert(); err != nil {
			return err
		}
		if err := fieldCatalog.SetString(fieldCatalogFieldTableName, tableName); err != nil {
			return err
		}
		if err := fieldCatalog.SetString(fieldCatalogFieldFieldName, fieldName); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(fieldCatalogFieldType, int32(schema.Type(fieldName))); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(fieldCatalogFieldLength, schema.Length(fieldName)); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(fieldCatalogFieldOffset, layout.Offset(fieldName)); err != nil {
			return err
		}
	}
	return nil
}

func (tm *TableManager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	tm.logger.Tracef("(%q) GetLayout", tableName)
	defer func() {
		tm.logger.Tracef("(%q) GetLayout: done", tableName)
	}()

	var size int32 = -1
	tableCatalog, err := query.NewTableScan(tx, tableCatalogTableName, tm.tableCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer tableCatalog.Close()

	for {
		next, err := tableCatalog.Next()
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		t, err := tableCatalog.GetString(tableCatalogFieldTableName)
		if err != nil {
			return nil, err
		}
		tm.logger.Tracef("(%q) GetLayout: tableCatalog.Next(): `%s`", tableName, t)
		if t == tableName {
			size, err = tableCatalog.GetInt(tableCatalogFieldSlotSize)
			if err != nil {
				return nil, err
			}
			tm.logger.Tracef("(%q) GetLayout: tableCatalog.Next(): size=%d", tableName, size)
			break
		}
	}

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fieldCatalog, err := query.NewTableScan(tx, fieldCatalogTableName, tm.fieldCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer fieldCatalog.Close()

	for {
		next, err := fieldCatalog.Next()
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		t, err := fieldCatalog.GetString(fieldCatalogFieldTableName)
		if err != nil {
			return nil, err
		}
		tm.logger.Tracef("(%q) GetLayout: fieldCatalog.Next(): `%s`", tableName, t)
		if t == tableName {
			fldname, err := fieldCatalog.GetString(fieldCatalogFieldFieldName)
			if err != nil {
				return nil, err
			}
			fldtype, err := fieldCatalog.GetInt(fieldCatalogFieldType)
			if err != nil {
				return nil, err
			}
			fldlen, err := fieldCatalog.GetInt(fieldCatalogFieldLength)
			if err != nil {
				return nil, err
			}
			offset, err := fieldCatalog.GetInt(fieldCatalogFieldOffset)
			if err != nil {
				return nil, err
			}
			tm.logger.Tracef("(%q) GetLayout: fieldCatalog.Next(): fldname=%q, fldtype=%d, fldlen=%d, offset=%d", tableName, fldname, fldtype, fldlen, offset)
			offsets[fldname] = offset
			schema.AddField(fldname, record.FieldType(fldtype), fldlen)
		}
	}
	return record.NewLayout(schema, offsets, size), nil
}
