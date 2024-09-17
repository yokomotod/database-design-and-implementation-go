package plan

import (
	"errors"
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
	tablePlan, err := NewTablePlan(tx, data.TableName, up.mdm)
	if err != nil {
		return 0, err
	}
	scan, err := tablePlan.Open()
	if err != nil {
		return 0, err
	}
	defer scan.Close()

	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, errors.New("ExecuteInsert: plan is not a table plan")
	}
	if err := updateScan.Insert(); err != nil {
		return 0, err
	}

	for i, field := range data.Fields {
		val := data.Values[i]
		if err := updateScan.SetVal(field, val); err != nil {
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

	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, errors.New("ExecuteDelete: plan is not a table plan")
	}
	count := 0
	for {
		if hasNext, err := scan.Next(); err != nil {
			return 0, err
		} else if !hasNext {
			break
		}

		if err := updateScan.Delete(); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(tx, data.TableName, up.mdm)
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

	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, errors.New("ExecuteModify: plan is not a table plan")
	}
	count := 0
	for {
		if hasNext, err := scan.Next(); err != nil {
			return 0, err
		} else if !hasNext {
			break
		}

		newVal, err := data.NewValue.Evaluate(scan)
		if err != nil {
			return 0, err
		}
		if err := updateScan.SetVal(data.TargetField, newVal); err != nil {
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
