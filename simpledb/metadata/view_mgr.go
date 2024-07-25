package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const maxViewDef = 100

type ViewMgr struct {
	tblMgr *TableMgr
}

func NewViewMgr(isNew bool, tblMgr *TableMgr, tx *tx.Transaction) (*ViewMgr, error) {
	viewMgr := &ViewMgr{tblMgr}
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", MaxName)
		schema.AddStringField("viewdef", maxViewDef)
		err := tblMgr.CreateTable("viewcat", schema, tx)
		if err != nil {
			return nil, err
		}
	}
	return viewMgr, nil
}

func (vm *ViewMgr) CreateView(vname string, vdef string, tx *tx.Transaction) error {
	layout, err := vm.tblMgr.GetLayout("viewcat", tx)
	if err != nil {
		return err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return err
	}
	ts.Insert() // 書籍の方に記載されておらず罠
	ts.SetString("viewname", vname)
	ts.SetString("viewdef", vdef)
	ts.Close()
	return nil
}

// 書籍と異なり見つからない場合は nil ではなく空文字を返す
func (vm *ViewMgr) GetViewDef(vname string, tx *tx.Transaction) (string, error) {
	result := ""
	layout, err := vm.tblMgr.GetLayout("viewcat", tx)
	if err != nil {
		return "", err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return "", err
	}
	next, err := ts.Next()
	if err != nil {
		return "", err
	}
	for next {
		viewname, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}
		if viewname == vname {
			result, err = ts.GetString("viewdef")
			if err != nil {
				return "", err
			}
			break
		}
		next, err = ts.Next()
		if err != nil {
			return "", err
		}
	}
	ts.Close()
	return result, nil
}
