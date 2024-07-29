package metadata

import (
	"simpledb/record"
	"simpledb/tx"
)

const maxViewDef = 100
const viewCatalogTableName = "viewcat"
const viewCatalogFieldViewName = "viewname"
const viewCatalogFieldViewDef = "viewdef"

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	viewManager := &ViewManager{tableManager}
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(viewCatalogFieldViewName, MaxName)
		schema.AddStringField(viewCatalogFieldViewDef, maxViewDef)
		err := tableManager.CreateTable(viewCatalogTableName, schema, tx)
		if err != nil {
			return nil, err
		}
	}
	return viewManager, nil
}

func (vm *ViewManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) error {
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName, tx)
	if err != nil {
		return err
	}
	ts, err := record.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return err
	}
	defer ts.Close()

	if err := ts.Insert(); err != nil { // 書籍の方に記載されておらず罠
		return err
	}
	if err := ts.SetString(viewCatalogFieldViewName, viewName); err != nil {
		return err
	}
	if err := ts.SetString(viewCatalogFieldViewDef, viewDef); err != nil {
		return err
	}
	return nil
}

// 書籍と異なり見つからない場合は nil ではなく空文字を返す
func (vm *ViewManager) GetViewDef(viewName string, tx *tx.Transaction) (string, error) {
	result := ""
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName, tx)
	if err != nil {
		return "", err
	}
	ts, err := record.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return "", err
	}
	defer ts.Close()

	next, err := ts.Next()
	if err != nil {
		return "", err
	}
	for next {
		viewname, err := ts.GetString(viewCatalogFieldViewName)
		if err != nil {
			return "", err
		}
		if viewname == viewName {
			result, err = ts.GetString(viewCatalogFieldViewDef)
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
