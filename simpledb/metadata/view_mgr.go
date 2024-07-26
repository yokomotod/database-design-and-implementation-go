package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const maxViewDef = 100

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	viewManager := &ViewManager{tableManager}
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", MaxName)
		schema.AddStringField("viewdef", maxViewDef)
		err := tableManager.CreateTable("viewcat", schema, tx)
		if err != nil {
			return nil, err
		}
	}
	return viewManager, nil
}

func (vm *ViewManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) error {
	layout, err := vm.tableManager.GetLayout("viewcat", tx)
	if err != nil {
		return err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return err
	}
	defer ts.Close()

	ts.Insert() // 書籍の方に記載されておらず罠
	ts.SetString("viewname", viewName)
	ts.SetString("viewdef", viewDef)
	return nil
}

// 書籍と異なり見つからない場合は nil ではなく空文字を返す
func (vm *ViewManager) GetViewDef(viewName string, tx *tx.Transaction) (string, error) {
	result := ""
	layout, err := vm.tableManager.GetLayout("viewcat", tx)
	if err != nil {
		return "", err
	}
	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return "", err
	}
	defer ts.Close()

	next, err := ts.Next()
	if err != nil {
		return "", err
	}
	for next {
		viewname, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}
		if viewname == viewName {
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
	return result, nil
}
