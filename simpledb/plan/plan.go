package plan

import (
	"encoding/json"
	"simpledb/query"
	"simpledb/record"
)

type PlanNode struct {
	Name          string `json:"name"`
	plan          Plan
	RecordsOutput int32       `json:"records_output"`
	Children      []*PlanNode `json:"children"`
}

func NewPlanNode(name string, plan Plan, children []*PlanNode) *PlanNode {
	return &PlanNode{
		Name:          name,
		plan:          plan,
		RecordsOutput: plan.RecordsOutput(),
		Children:      children,
	}
}

func (n *PlanNode) String() string {
	bytes, err := json.MarshalIndent(n, "", "    ")
	if err != nil {
		return "error"
	}
	return string(bytes)
}

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int32
	RecordsOutput() int32
	DistinctValues(fieldName string) int32
	Schema() *record.Schema

	Tree() *PlanNode
}
