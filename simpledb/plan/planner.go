package plan

import (
	"fmt"
	"simpledb/parse"
	"simpledb/tx"
)

type QueryPlanner interface {
	CreatePlan(querydata *parse.QueryData, tx *tx.Transaction) (Plan, error)
}

type UpdatePlanner interface {
	ExecuteInsert(insertdata *parse.InsertData, tx *tx.Transaction) (int, error)
	ExecuteDelete(deletedata *parse.DeleteData, tx *tx.Transaction) (int, error)
	ExecuteModify(modifydata *parse.ModifyData, tx *tx.Transaction) (int, error)
	ExecuteCreateTable(createtabledata *parse.CreateTableData, tx *tx.Transaction) (int, error)
	ExecuteCreateView(createviewdata *parse.CreateViewData, tx *tx.Transaction) (int, error)
	ExecuteCreateIndex(createindexdata *parse.CreateIndexData, tx *tx.Transaction) (int, error)
}

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{
		queryPlanner:  queryPlanner,
		updatePlanner: updatePlanner,
	}
}

func (p *Planner) CreateQueryPlan(query string, tx *tx.Transaction) (Plan, error) {
	parser, err := parse.NewParser(query)
	if err != nil {
		return nil, err
	}
	querydata, err := parser.Query()
	if err != nil {
		return nil, err
	}

	err = p.verifyQuery(querydata)
	if err != nil {
		return nil, err
	}

	return p.queryPlanner.CreatePlan(querydata, tx)
}

func (p *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) (int, error) {
	parser, err := parse.NewParser(cmd)
	if err != nil {
		return 0, err
	}

	updateCmd, err := parser.UpdateCmd()
	if err != nil {
		return 0, err
	}

	err = p.verifyUpdate(updateCmd)
	if err != nil {
		return 0, err
	}

	switch cmd := updateCmd.(type) {
	case *parse.InsertData:
		return p.updatePlanner.ExecuteInsert(cmd, tx)
	case *parse.DeleteData:
		return p.updatePlanner.ExecuteDelete(cmd, tx)
	case *parse.ModifyData:
		return p.updatePlanner.ExecuteModify(cmd, tx)
	case *parse.CreateTableData:
		return p.updatePlanner.ExecuteCreateTable(cmd, tx)
	case *parse.CreateViewData:
		return p.updatePlanner.ExecuteCreateView(cmd, tx)
	case *parse.CreateIndexData:
		return p.updatePlanner.ExecuteCreateIndex(cmd, tx)
	default:
		return 0, fmt.Errorf("unexpected update command: %v", cmd)
	}
}

func (p *Planner) verifyQuery(queryData *parse.QueryData) error {
	// TODO implement
	return nil
}

func (p *Planner) verifyUpdate(updateCmd parse.UpdateCmd) error {
	// TODO implement
	return nil
}
