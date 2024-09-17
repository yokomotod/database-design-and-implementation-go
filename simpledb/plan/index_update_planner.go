package plan

import (
	"errors"
	"simpledb/metadata"
	"simpledb/parse"
	"simpledb/query"
	"simpledb/tx"
)

var _ UpdatePlanner = (*IndexUpdatePlanner)(nil)

type IndexUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewIndexUpdatePlanner(mdm *metadata.Manager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{mdm}
}

func (up *IndexUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int, error) {
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

	rid, err := updateScan.GetRID()
	if err != nil {
		return 0, err
	}
	indexes, err := up.mdm.GetIndexInfo(data.TableName, tx)
	if err != nil {
		return 0, err
	}

	for i, field := range data.Fields {
		val := data.Values[i]
		if err := updateScan.SetVal(field, val); err != nil {
			return 0, err
		}

		ii, ok := indexes[field]
		if !ok {
			continue
		}

		idx, err := ii.Open()
		if err != nil {
			return 0, err
		}
		if err := idx.Insert(val, rid); err != nil {
			return 0, err
		}
		idx.Close()
	}
	return 1, nil
}

func (up *IndexUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	tablePlan, err := NewTablePlan(tx, tableName, up.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan, err := NewSelectPlan(tablePlan, data.Pred)
	if err != nil {
		return 0, err
	}

	indexes, err := up.mdm.GetIndexInfo(tableName, tx)
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

		rid, err := updateScan.GetRID()
		if err != nil {
			return 0, err
		}
		for fieldName, ii := range indexes {
			val, err := scan.GetVal(fieldName)
			if err != nil {
				return 0, err
			}
			idx, err := ii.Open()
			if err != nil {
				return 0, err
			}
			if err := idx.Delete(val, rid); err != nil {
				return 0, err
			}
			idx.Close()
		}

		if err := updateScan.Delete(); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (up *IndexUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(tx, data.TableName, up.mdm)
	if err != nil {
		return 0, err
	}

	selectPlan, err := NewSelectPlan(tablePlan, data.Pred)
	if err != nil {
		return 0, err
	}

	indexInfoMap, err := up.mdm.GetIndexInfo(data.TableName, tx)
	if err != nil {
		return 0, err
	}
	var idx query.Index
	if indexInfo, ok := indexInfoMap[data.TargetField]; ok {
		idx, err = indexInfo.Open()
		if err != nil {
			return 0, err
		}
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
		oldVal, err := scan.GetVal(data.TargetField)
		if err != nil {
			return 0, err
		}
		if err := updateScan.SetVal(data.TargetField, newVal); err != nil {
			return 0, err
		}

		count++

		if idx == nil {
			continue
		}

		rid, err := updateScan.GetRID()
		if err != nil {
			return 0, err
		}
		if err := idx.Delete(oldVal, rid); err != nil {
			return 0, err
		}
		if err := idx.Insert(newVal, rid); err != nil {
			return 0, err
		}
	}

	if idx != nil {
		idx.Close()
	}

	return count, nil
}

func (up *IndexUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	schema := data.NewSchema
	err := up.mdm.CreateTable(tableName, schema, tx)
	return 0, err
}

func (up *IndexUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) (int, error) {
	viewName := data.ViewName
	viewDef := data.ViewDef()
	err := up.mdm.CreateView(viewName, viewDef, tx)
	return 0, err
}

func (up *IndexUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) (int, error) {
	indexName := data.IndexName
	tableName := data.TableName
	fieldName := data.FieldName
	err := up.mdm.CreateIndex(indexName, tableName, fieldName, tx)
	return 0, err
}
