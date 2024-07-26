package metadata

import (
	"simpledb/record"
	"simpledb/tx"
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
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
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

	tableManager := &TableManager{tableCatalogLayout, fieldCatalogLayout}
	if isNew {
		tableManager.CreateTable(tableCatalogTableName, tableCatalogSchema, tx)
		tableManager.CreateTable(fieldCatalogTableName, fieldCatalogSchema, tx)
	}
	return tableManager
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayoutFromSchema(schema)
	tableCatalog, err := record.NewTableScan(tx, tableCatalogTableName, tm.tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalog.Close()

	tableCatalog.Insert()
	tableCatalog.SetString(tableCatalogFieldTableName, tableName)
	tableCatalog.SetInt(tableCatalogFieldSlotSize, layout.SlotSize())

	fieldCatalog, err := record.NewTableScan(tx, fieldCatalogTableName, tm.fieldCatalogLayout)
	if err != nil {
		return err
	}
	defer fieldCatalog.Close()

	for _, fieldName := range schema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString(fieldCatalogFieldTableName, tableName)
		fieldCatalog.SetString(fieldCatalogFieldFieldName, fieldName)
		fieldCatalog.SetInt(fieldCatalogFieldType, int32(schema.Type(fieldName)))
		fieldCatalog.SetInt(fieldCatalogFieldLength, schema.Length(fieldName))
		fieldCatalog.SetInt(fieldCatalogFieldOffset, layout.Offset(fieldName))
	}
	return nil
}

func (tm *TableManager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	var size int32 = -1
	tableCatalog, err := record.NewTableScan(tx, tableCatalogTableName, tm.tableCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer tableCatalog.Close()

	next, err := tableCatalog.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := tableCatalog.GetString(tableCatalogFieldTableName)
		if err != nil {
			return nil, err
		}
		if t == tableName {
			size, err = tableCatalog.GetInt(tableCatalogFieldSlotSize)
			if err != nil {
				return nil, err
			}
			break
		}
		next, err = tableCatalog.Next()
		if err != nil {
			return nil, err
		}
	}

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fieldCatalog, err := record.NewTableScan(tx, fieldCatalogTableName, tm.fieldCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer fieldCatalog.Close()

	next, err = fieldCatalog.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := fieldCatalog.GetString(fieldCatalogFieldTableName)
		if err != nil {
			return nil, err
		}
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
			offsets[fldname] = offset
			schema.AddField(fldname, record.FieldType(fldtype), fldlen)
		}
		next, err = fieldCatalog.Next()
		if err != nil {
			return nil, err
		}
	}
	return record.NewLayout(schema, offsets, size), nil
}
