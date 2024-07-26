package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const (
	MaxName = 16
)

type TableMgr struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

func NewTableMgr(isNew bool, tx *tx.Transaction) *TableMgr {
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MaxName)
	tcatSchema.AddIntField("slotsize")
	tcatLayout := record.NewLayoutFromSchema(tcatSchema)
	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MaxName)
	fcatSchema.AddStringField("fldname", MaxName)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := record.NewLayoutFromSchema(fcatSchema)
	tableMgr := &TableMgr{tcatLayout, fcatLayout}
	if isNew {
		tableMgr.CreateTable("tblcat", tcatSchema, tx)
		tableMgr.CreateTable("fldcat", fcatSchema, tx)
	}
	return tableMgr
}

func (tm *TableMgr) CreateTable(tblname string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayoutFromSchema(schema)
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return err
	}
	defer tcat.Close()

	tcat.Insert()
	tcat.SetString("tblname", tblname)
	tcat.SetInt("slotsize", layout.SlotSize())

	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return err
	}
	defer fcat.Close()

	for _, fieldName := range schema.Fields() {
		fcat.Insert()
		fcat.SetString("tblname", tblname)
		fcat.SetString("fldname", fieldName)
		fcat.SetInt("type", int32(schema.Type(fieldName)))
		fcat.SetInt("length", schema.Length(fieldName))
		fcat.SetInt("offset", layout.Offset(fieldName))
	}
	return nil
}

func (tm *TableMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	var size int32 = -1
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return nil, err
	}
	defer tcat.Close()

	next, err := tcat.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := tcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if t == tblname {
			size, err = tcat.GetInt("slotsize")
			if err != nil {
				return nil, err
			}
			break
		}
		next, err = tcat.Next()
		if err != nil {
			return nil, err
		}
	}

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return nil, err
	}
	defer fcat.Close()

	next, err = fcat.Next()
	if err != nil {
		return nil, err
	}
	for next {
		t, err := fcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if t == tblname {
			fldname, err := fcat.GetString("fldname")
			if err != nil {
				return nil, err
			}
			fldtype, err := fcat.GetInt("type")
			if err != nil {
				return nil, err
			}
			fldlen, err := fcat.GetInt("length")
			if err != nil {
				return nil, err
			}
			offset, err := fcat.GetInt("offset")
			if err != nil {
				return nil, err
			}
			offsets[fldname] = offset
			schema.AddField(fldname, record.FieldType(fldtype), fldlen)
		}
		next, err = fcat.Next()
		if err != nil {
			return nil, err
		}
	}
	return record.NewLayout(schema, offsets, size), nil
}
