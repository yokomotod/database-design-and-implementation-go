package plan

import (
	"fmt"
	"simpledb/metadata"
	"simpledb/parse"
	"simpledb/query"
	"simpledb/tx"
)

var _ UpdatePlanner = (*BasicUpdatePlanner)(nil)

type BasicUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewBasicUpdatePlanner(mdm *metadata.Manager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

func (up *BasicUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	tablePlan, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}

	scan, err := tablePlan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()

	us, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("cannot insert: %v", data)
	}
	err = us.Insert()
	if err != nil {
		return 0, err
	}
	for i, field := range data.Fields {
		value := data.Values[i]
		err = us.SetVal(field, value)
		if err != nil {
			return 0, err
		}
	}
	return 1, nil
}

func (up *BasicUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	tablePlan, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan, err := NewSelectPlan(tablePlan, data.Pred)
	if err != nil {
		return 0, err
	}

	scan, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()

	us, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("cannot delete: %v", data)
	}
	count := 0
	for {
		hasNext, err := scan.Next()
		if err != nil {
			return 0, err
		}
		if !hasNext {
			break
		}
		err = us.Delete()
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	tablePlan, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan, err := NewSelectPlan(tablePlan, data.Pred)
	if err != nil {
		return 0, err
	}

	scan, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()

	us, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("cannot modify: %v", data)
	}
	count := 0
	for {
		hasNext, err := scan.Next()
		if err != nil {
			return 0, err
		}
		if !hasNext {
			break
		}
		newValue, err := data.NewValue.Evaluate(scan)
		if err != nil {
			return 0, err
		}
		err = us.SetVal(data.TargetField, newValue)
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	schema := data.NewSchema
	err := up.mdm.CreateTable(tableName, schema, tx)
	return 0, err
}

func (up *BasicUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int, error) {
	viewName := data.ViewName
	viewDef := data.ViewDef()
	err := up.mdm.CreateView(viewName, viewDef, tx)
	return 0, err
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int, error) {
	indexName := data.IndexName
	tableName := data.TableName
	fieldName := data.FieldName
	err := up.mdm.CreateIndex(indexName, tableName, fieldName, tx)
	return 0, err
}
