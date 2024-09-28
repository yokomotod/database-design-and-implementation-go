package plan

import (
	"fmt"
	"simpledb/metadata"
	"simpledb/parse"
	"simpledb/query"
	"simpledb/record"
	"simpledb/tx"
	"slices"
)

type TablePlanner struct {
	myPlan   *TablePlan
	myPred   *query.Predicate
	mySchema *record.Schema

	indexes map[string]*metadata.IndexInfo
	tx      *tx.Transaction
}

// Creates a new table planner.
// The specified predicate applies to the entire query.
// The table planner is responsible for determining
// which portion of the predicate is useful to the table,
// and when indexes are useful.
func NewTablePlanner(tblName string, pred *query.Predicate, tx *tx.Transaction, mdm *metadata.Manager) (*TablePlanner, error) {
	myPlan, err := NewTablePlan(tx, tblName, mdm)
	if err != nil {
		return nil, fmt.Errorf("NewTablePlan: %w", err)
	}
	mySchema := myPlan.Schema()
	indexes, err := mdm.GetIndexInfo(tblName, tx)
	if err != nil {
		return nil, fmt.Errorf("mdm.GetIndexInfo: %w", err)
	}

	return &TablePlanner{
		myPlan:   myPlan,
		myPred:   pred,
		mySchema: mySchema,
		indexes:  indexes,
		tx:       tx,
	}, nil
}

// Constructs a select plan for the table.
// The plan will use an indexselect, if possible.
func (tp *TablePlanner) MakeSelectPlan() (Plan, error) {
	p := tp.makeIndexSelect()
	if p == nil {
		p = tp.myPlan
	}

	s, err := tp.addSelectPred(p)
	if err != nil {
		return nil, fmt.Errorf("tp.addSelectPred: %w", err)
	}

	return s, nil
}

// Constructs a join plan of the specified plan and the table.
// The plan will use an indexjoin, if possible.
// (Which means that if an indexselect is also possible, the indexjoin operator takes precedence.)
// The method returns null if no join is possible.
func (tp *TablePlanner) MakeJoinPlan(current Plan) (Plan, error) {
	currSch := current.Schema()
	joinPred := tp.myPred.JoinSubPred(tp.mySchema, currSch)
	if joinPred == nil {
		return nil, nil
	}

	p, err := tp.makeIndexJoin(current, currSch)
	if err != nil {
		return nil, fmt.Errorf("tp.makeIndexJoin: %w", err)
	}
	if p != nil {
		return p, nil
	}

	// Exercise 15.17: suport HashJoin

	// and MergeJoin ?

	p, err = tp.makeProductJoin(current, currSch)
	if err != nil {
		return nil, fmt.Errorf("tp.makeProductJoin: %w", err)
	}

	return p, nil
}

// Constructs a product plan of the specified plan and this table.
func (tp *TablePlanner) makeProductPlan(current Plan) (Plan, error) {
	p, err := tp.addSelectPred(tp.myPlan)
	if err != nil {
		return nil, fmt.Errorf("tp.addSelectPred: %w", err)
	}

	return NewMultibufferProductPlan(tp.tx, current, p), nil
}

func (tp *TablePlanner) makeIndexSelect() Plan {
	for fldName := range tp.indexes {
		val := tp.myPred.EquatesWithConstant(fldName)
		if val == nil {
			continue
		}

		ii := tp.indexes[fldName]
		fmt.Println("index on", fldName, "used")
		return NewIndexSelectPlan(tp.myPlan, ii, val)
	}

	return nil
}

func (tp *TablePlanner) makeIndexJoin(current Plan, currSch *record.Schema) (Plan, error) {
	for fldName := range tp.indexes {
		outerField := tp.myPred.EquatesWithField(fldName)
		if outerField == "" || !currSch.HasField(outerField) {
			continue
		}

		var p Plan = NewIndexJoinPlan(current, tp.myPlan, tp.indexes[fldName], outerField)

		p, err := tp.addSelectPred(p)
		if err != nil {
			return nil, fmt.Errorf("tp.addSelectPred: %w", err)
		}

		return p, nil
	}

	return nil, nil
}

func (tp *TablePlanner) makeProductJoin(current Plan, currSch *record.Schema) (Plan, error) {
	p, err := tp.makeProductPlan(current)
	if err != nil {
		return nil, fmt.Errorf("tp.makeProductPlan: %w", err)
	}

	p, err = tp.addJoinPred(p, currSch)
	if err != nil {
		return nil, fmt.Errorf("tp.addJoinPred: %w", err)
	}

	return p, nil
}

func (tp *TablePlanner) addSelectPred(p Plan) (Plan, error) {
	selectPred := tp.myPred.SelectSubPred(tp.mySchema)
	if selectPred == nil {
		return p, nil
	}

	selectPlan, err := NewSelectPlan(p, selectPred)
	if err != nil {
		return nil, fmt.Errorf("NewSelectPlan: %w", err)
	}

	return selectPlan, nil
}

func (tp *TablePlanner) addJoinPred(p Plan, currSch *record.Schema) (Plan, error) {
	joinPred := tp.myPred.JoinSubPred(currSch, tp.mySchema)
	if joinPred == nil {
		return p, nil
	}

	selectPlan, err := NewSelectPlan(p, joinPred)
	if err != nil {
		return nil, fmt.Errorf("NewSelectPlan: %w", err)
	}

	return selectPlan, nil
}

var _ QueryPlanner = (*HeuristicQueryPlanner)(nil)

type HeuristicQueryPlanner struct {
	tablePlanners []*TablePlanner
	mdm           *metadata.Manager
}

func NewHeuristicQueryPlanner(mdm *metadata.Manager) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{
		mdm: mdm,
	}
}

// Creates an optimized left-deep query plan using the following heuristics.
//   - H1. Choose the smallest table (considering selection predicates) to be first in the join order.
//   - H2. Add the table to the join order which results in the smallest output.
func (h *HeuristicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) (Plan, error) {
	// Step 1: Create a TablePlanner object for each mentioned table
	tablePlanners := make([]*TablePlanner, 0, len(data.Tables))
	for _, tblName := range data.Tables {
		tp, err := NewTablePlanner(tblName, data.Pred, tx, h.mdm)
		if err != nil {
			return nil, fmt.Errorf("NewTablePlanner: %w", err)
		}
		tablePlanners = append(tablePlanners, tp)
	}
	h.tablePlanners = tablePlanners

	// Step 2: Choose the lowest-size plan to begin the join order
	currentPlan, err := h.getLowestSelectPlan()
	if err != nil {
		return nil, fmt.Errorf("h.getLowestSelectPlan: %w", err)
	}

	// Step 3: Repeatedly add a plan to the join order
	for len(h.tablePlanners) > 0 {
		p, err := h.getLowestJoinPlan(currentPlan)
		if err != nil {
			return nil, fmt.Errorf("h.getLowestJoinPlan: %w", err)
		}
		if p != nil {
			currentPlan = p
			continue
		}

		// no applicable join
		p, err = h.getLowestProductPlan(currentPlan)
		if err != nil {
			return nil, fmt.Errorf("h.getLowestProductPlan: %w", err)
		}
		currentPlan = p
	}

	// Step 4. Project on the field names and return
	p, err := NewProjectPlan(currentPlan, data.Fields)
	if err != nil {
		return nil, fmt.Errorf("plan.NewProjectPlan: %w", err)
	}

	return p, nil
}

func (h *HeuristicQueryPlanner) getLowestSelectPlan() (Plan, error) {
	bestIndex := -1
	var bestPlan Plan
	for i, tp := range h.tablePlanners {
		plan, err := tp.MakeSelectPlan()
		if err != nil {
			return nil, fmt.Errorf("tp.MakeSelectPlan: %w", err)
		}
		if bestPlan != nil && plan.RecordsOutput() >= bestPlan.RecordsOutput() {
			continue
		}

		bestIndex = i
		bestPlan = plan
	}

	h.tablePlanners = slices.Delete(h.tablePlanners, bestIndex, bestIndex+1)

	return bestPlan, nil
}

func (h *HeuristicQueryPlanner) getLowestJoinPlan(current Plan) (Plan, error) {
	bestIndex := -1
	var bestPlan Plan
	for i, tp := range h.tablePlanners {
		plan, err := tp.MakeJoinPlan(current)
		if err != nil {
			return nil, fmt.Errorf("tp.MakeJoinPlan: %w", err)
		}
		if plan == nil {
			continue
		}
		if bestPlan != nil && plan.RecordsOutput() >= bestPlan.RecordsOutput() {
			continue
		}

		bestIndex = i
		bestPlan = plan
	}

	if bestIndex >= 0 {
		h.tablePlanners = slices.Delete(h.tablePlanners, bestIndex, bestIndex+1)
	}

	return bestPlan, nil
}

func (h *HeuristicQueryPlanner) getLowestProductPlan(current Plan) (Plan, error) {
	bestIndex := -1
	var bestPlan Plan
	for i, tp := range h.tablePlanners {
		plan, err := tp.makeProductPlan(current)
		if err != nil {
			return nil, fmt.Errorf("tp.makeProductPlan: %w", err)
		}
		if bestPlan != nil && plan.RecordsOutput() >= bestPlan.RecordsOutput() {
			continue
		}

		bestIndex = i
		bestPlan = plan
	}

	h.tablePlanners = slices.Delete(h.tablePlanners, bestIndex, bestIndex+1)

	return bestPlan, nil
}
