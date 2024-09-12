package plan

import (
	"errors"
	"log"
	"simpledb/metadata"
	"simpledb/parse"
	"simpledb/query"
	"simpledb/tx"
)

type IndexUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewIndexUpdatePlanner(mdm *metadata.Manager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{mdm}
}

func (iup *IndexUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) (int, error) {
	tblname := data.TableName
	plan, err := NewTablePlan(tx, tblname, iup.mdm)
	if err != nil {
		return 0, err
	}
	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
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
	indexes, err := iup.mdm.GetIndexInfo(tblname, tx)
	if err != nil {
		return 0, err
	}
	for i, fieldName := range data.Fields {
		val := data.Values[i]
		log.Printf("Modify field %s to value %v", fieldName, val)
		if err := updateScan.SetVal(fieldName, val); err != nil {
			return 0, err
		}
		if ii, ok := indexes[fieldName]; ok {
			idx, err := ii.Open()
			if err != nil {
				return 0, err
			}
			if err := idx.Insert(val, rid); err != nil {
				return 0, err
			}
			idx.Close()
		}
	}
	scan.Close()
	return 1, nil
}

func (iup *IndexUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	plan, err := NewTablePlan(tx, tableName, iup.mdm)
	if err != nil {
		return 0, err
	}
	indexes, err := iup.mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return 0, err
	}
	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, errors.New("ExecuteDelete: plan is not a table plan")
	}
	count := 0
	for {
		if ok, err := scan.Next(); err != nil {
			return 0, err
		} else if !ok {
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
	scan.Close()
	return count, nil
}

func (iup *IndexUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) (int, error) {
	tableName := data.TableName
	fieldName := data.TargetField
	tablePlan, err := NewTablePlan(tx, tableName, iup.mdm)
	if err != nil {
		return 0, err
	}
	plan, err := NewSelectPlan(tablePlan, data.Pred)
	if err != nil {
		return 0, err
	}
	indexInfoMap, err := iup.mdm.GetIndexInfo(tableName, tx)
	if err != nil {
		return 0, err
	}
	indexInfo, ok := indexInfoMap[fieldName]
	var idx query.Index
	if ok {
		idx, err = indexInfo.Open()
		if err != nil {
			return 0, err
		}
	}
	scan, err := plan.Open()
	if err != nil {
		return 0, err
	}
	updateScan, ok := scan.(query.UpdateScan)
	if !ok {
		return 0, errors.New("ExecuteModify: plan is not a table plan")
	}
	count := 0
	for {
		if ok, err := scan.Next(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		newVal, err := data.NewValue.Evaluate(scan)
		if err != nil {
			return 0, err
		}
		oldVal, err := scan.GetVal(fieldName)
		if err != nil {
			return 0, err
		}
		if err := updateScan.SetVal(data.TargetField, newVal); err != nil {
			return 0, err
		}
		if idx != nil {
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
		count++
	}
	if idx != nil {
		idx.Close()
	}
	scan.Close()
	return count, nil
}
