package parse

import (
	"fmt"
	"simpledb/query"
	"simpledb/record"
	"strings"
)

// QueryData SELECT文
type QueryData struct {
	Fields []string
	Tables []string
	Pred   *query.Predicate
}

func NewQueryData(fields, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		Fields: fields,
		Tables: tables,
		Pred:   pred,
	}
}

func (q *QueryData) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "select %s from %s", strings.Join(q.Fields, ", "), strings.Join(q.Tables, ", "))

	if pred := q.Pred.String(); pred != "" {
		fmt.Fprintf(&sb, " where %s", pred)
	}

	return sb.String()
}

// UpdateCmd 更新系のコマンド
// MEMO: もとのjavaコードではinterface無しでObject型を返している。interface持つように改善
type UpdateCmd interface {
	updateCmd()
}

func (*InsertData) updateCmd()      {}
func (*ModifyData) updateCmd()      {}
func (*DeleteData) updateCmd()      {}
func (*CreateTableData) updateCmd() {}
func (*CreateViewData) updateCmd()  {}
func (*CreateIndexData) updateCmd() {}

// InsertData INSERT文
type InsertData struct {
	TableName string
	Fields    []string
	Values    []*query.Constant
}

func NewInsertData(tableName string, fields []string, values []*query.Constant) *InsertData {
	return &InsertData{
		TableName: tableName,
		Fields:    fields,
		Values:    values,
	}
}

// ModifyData UPDATE文
type ModifyData struct {
	TableName   string
	TargetField string
	NewValue    *query.Expression
	Pred        *query.Predicate
}

func NewModifyData(tableName string, targetField string, newValue *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		TableName:   tableName,
		TargetField: targetField,
		NewValue:    newValue,
		Pred:        pred,
	}
}

// DeleteData DELETE文
type DeleteData struct {
	TableName string
	Pred      *query.Predicate
}

func NewDeleteData(tableName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		TableName: tableName,
		Pred:      pred,
	}
}

// CreateTableData CREATE TABLE文
type CreateTableData struct {
	TableName string
	NewSchema *record.Schema
}

func NewCreateTableData(tableName string, newSchema *record.Schema) *CreateTableData {
	return &CreateTableData{
		TableName: tableName,
		NewSchema: newSchema,
	}
}

// CreateViewData CREATE VIEW文
type CreateViewData struct {
	ViewName  string
	QueryData *QueryData
}

func NewCreateViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{
		ViewName:  viewName,
		QueryData: queryData,
	}
}

func (cv *CreateViewData) ViewDef() string {
	return cv.QueryData.String()
}

// CreateIndexData CREATE INDEX文
type CreateIndexData struct {
	IndexName string
	TableName string
	FieldName string
}

func NewCreateIndexData(indexName string, tableName string, fieldName string) *CreateIndexData {
	return &CreateIndexData{
		IndexName: indexName,
		TableName: tableName,
		FieldName: fieldName,
	}
}
