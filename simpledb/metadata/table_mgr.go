package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const (
	MaxName = 16
)

type TableManager struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStringField("tblname", MaxName)
	tableCatalogSchema.AddIntField("slotsize")
	tableCatalogLayout := record.NewLayoutFromSchema(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStringField("tblname", MaxName)
	fieldCatalogSchema.AddStringField("fldname", MaxName)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := record.NewLayoutFromSchema(fieldCatalogSchema)

	tableManager := &TableManager{tableCatalogLayout, fieldCatalogLayout}
	if isNew {
		tableManager.CreateTable("tblcat", tableCatalogSchema, tx)
		tableManager.CreateTable("fldcat", fieldCatalogSchema, tx)
	}
	return tableManager
}

func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayoutFromSchema(schema)
	tableCatalog, err := record.NewTableScan(tx, "tblcat", tm.tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalog.Close()

	tableCatalog.Insert()
	tableCatalog.SetString("tblname", tableName)
	tableCatalog.SetInt("slotsize", layout.SlotSize())

	fieldCatalog, err := record.NewTableScan(tx, "fldcat", tm.fieldCatalogLayout)
	if err != nil {
		return err
	}
	defer fieldCatalog.Close()

	for _, fieldName := range schema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString("tblname", tableName)
		fieldCatalog.SetString("fldname", fieldName)
		fieldCatalog.SetInt("type", int32(schema.Type(fieldName)))
		fieldCatalog.SetInt("length", schema.Length(fieldName))
		fieldCatalog.SetInt("offset", layout.Offset(fieldName))
	}
	return nil
}

func (tm *TableManager) GetLayout(tableName string, tx *tx.Transaction) (*record.Layout, error) {
	var size int32 = -1
	tableCatalog, err := record.NewTableScan(tx, "tblcat", tm.tableCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer tableCatalog.Close()

	next, err := tableCatalog.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := tableCatalog.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if t == tableName {
			size, err = tableCatalog.GetInt("slotsize")
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
	fieldCatalog, err := record.NewTableScan(tx, "fldcat", tm.fieldCatalogLayout)
	if err != nil {
		return nil, err
	}
	defer fieldCatalog.Close()

	next, err = fieldCatalog.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := fieldCatalog.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if t == tableName {
			fldname, err := fieldCatalog.GetString("fldname")
			if err != nil {
				return nil, err
			}
			fldtype, err := fieldCatalog.GetInt("type")
			if err != nil {
				return nil, err
			}
			fldlen, err := fieldCatalog.GetInt("length")
			if err != nil {
				return nil, err
			}
			offset, err := fieldCatalog.GetInt("offset")
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
