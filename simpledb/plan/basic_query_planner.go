package plan

import (
	"simpledb/metadata"
	"simpledb/parse"
	"simpledb/tx"
)

var _ QueryPlanner = (*BasicQueryPlanner)(nil)

type BasicQueryPlanner struct {
	mdm *metadata.Manager
}

func NewBasicQueryPlanner(mdm *metadata.Manager) *BasicQueryPlanner {
	return &BasicQueryPlanner{mdm: mdm}
}

func (qp *BasicQueryPlanner) CreatePlan(querydata *parse.QueryData, tx *tx.Transaction) (Plan, error) {
	var result Plan
	var err error

	plans := make([]Plan, 0, 5)

	// Step 1: テーブル・ビューに対するPlanの作成

	for _, tableName := range querydata.Tables {
		viewDef, err := qp.mdm.GetViewDef(tableName, tx)
		// View定義が存在しない場合から文字列
		if err == nil && viewDef != "" {
			// Viewの場合再帰的にPlanを作成
			parser, err := parse.NewParser(viewDef)
			if err != nil {
				return nil, err
			}
			viewdata, err := parser.Query()
			if err != nil {
				return nil, err
			}
			plan, err := qp.CreatePlan(viewdata, tx)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		} else {
			// テーブルの場合
			plan, err := NewTablePlan(tx, tableName, qp.mdm)
			if err != nil {
				return nil, err
			}
			plans = append(plans, plan)
		}
	}

	// Step 2: Step1のPlansに対するProduct Planを生成
	result = plans[0]
	for _, plan := range plans[1:] {
		result, err = NewProductPlan(result, plan)
		if err != nil {
			return nil, err
		}
	}

	// Step 3: Step2の結果に Pred を適用したSelect Planを生成
	result, err = NewSelectPlan(result, querydata.Pred)
	if err != nil {
		return nil, err
	}

	// Step 4: 指定フィールドを取り出すProjection Planを生成
	result, err = NewProjectPlan(result, querydata.Fields)

	return result, err
}
