package parse

import (
	"simpledb/query"
	"simpledb/record"
)

type Parser struct {
	lex *Lexer
}

func NewParser(input string) (*Parser, error) {
	lex, err := NewLexer(input)
	if err != nil {
		return nil, err
	}

	return &Parser{
		lex: lex,
	}, nil
}

// 述語の構文解析

// <Field> := IdTok
func (p *Parser) Field() (string, error) {
	// IdTok
	fieldName, err := p.lex.EatIdentifier()
	if err != nil {
		return "", err
	}

	return fieldName, nil
}

// <Constant> := StrTok | IntTok
func (p *Parser) Constant() (*query.Constant, error) {
	if p.lex.MatchStringConstant() {
		// StrTok
		value, err := p.lex.EatStringConstant()
		if err != nil {
			return nil, err
		}

		return query.NewConstantWithString(value), nil
	} else {
		// IntTok
		value, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}

		return query.NewConstantWithInt(value), nil
	}
}

// <Expression> := <Field> | <Constant>
func (p *Parser) Expression() (*query.Expression, error) {
	if p.lex.MatchIdentifier() {
		// <Field>
		fieldName, err := p.Field()
		if err != nil {
			return nil, err
		}

		return query.NewExpressionWithField(fieldName), nil
	} else {
		// Constant
		value, err := p.Constant()
		if err != nil {
			return nil, err
		}

		return query.NewExpressionWithConstant(value), nil
	}
}

// <Term> := <Expression> = <Expression>
func (p *Parser) Term() (*query.Term, error) {
	// <Expression>
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}

	// =
	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}

	// <Expression>
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}

	return query.NewTerm(lhs, rhs), nil
}

// <Predicate> := <Term> [ AND <Predicate> ]
func (p *Parser) Predicate() (*query.Predicate, error) {
	// <Term>
	term, err := p.Term()
	if err != nil {
		return nil, err
	}

	pred := query.NewPredicateWithTerm(term)

	// [ AND <Predicate> ]
	if p.lex.MatchKeyword("and") {
		// AND
		if err := p.lex.EatKeyword("and"); err != nil {
			return nil, err
		}

		// <Predicate>
		rhs, err := p.Predicate()
		if err != nil {
			return nil, err
		}

		pred.ConjoinWith(rhs)
	}

	return pred, nil
}

// クエリの構文解析

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
func (p *Parser) Query() (*QueryData, error) {
	// SELECT
	if err := p.lex.EatKeyword("select"); err != nil {
		return nil, err
	}

	// <SelectList>
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}

	// FROM
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}

	// <TableList>
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}

	// [ WHERE <Predicate> ]
	pred, err := p.whereOpt()
	if err != nil {
		return nil, err
	}

	return NewQueryData(fields, tables, pred), nil
}

// [ WHERE <Predicate> ]
func (p *Parser) whereOpt() (*query.Predicate, error) {
	if p.lex.MatchKeyword("where") {
		// WHERE
		if err := p.lex.EatKeyword("where"); err != nil {
			return nil, err
		}

		// <Predicate>
		pred, err := p.Predicate()
		if err != nil {
			return nil, err
		}

		return pred, nil
	} else {
		return query.NewPredicate(), nil
	}
}

// <SelectList> := <Field> [ , <SelectList> ]
func (p *Parser) selectList() ([]string, error) {
	// <Field>
	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	fields := []string{field}

	// [ , <SelectList> ]
	if p.lex.MatchDelim(',') {
		// ,
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		// <SelectList>
		rest, err := p.selectList()
		if err != nil {
			return nil, err
		}

		fields = append(fields, rest...)
	}

	return fields, nil
}

// <TableList> := IdTok [ , <TableList> ]
func (p *Parser) tableList() ([]string, error) {
	// IdTok
	table, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	tables := []string{table}

	// [ , <TableList> ]
	if p.lex.MatchDelim(',') {
		// ,
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		// <TableList>
		rest, err := p.tableList()
		if err != nil {
			return nil, err
		}

		tables = append(tables, rest...)
	}

	return tables, nil
}

// 更新コマンドの構文解析

// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
func (p *Parser) UpdateCmd() (UpdateCmd, error) {
	if p.lex.MatchKeyword("insert") {
		// <Insert>
		return p.Insert()
	} else if p.lex.MatchKeyword("delete") {
		// <Delete>
		return p.Delete()
	} else if p.lex.MatchKeyword("update") {
		// <Modify>
		return p.Modify()
	} else {
		// <Create>
		return p.create()
	}
}

// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
func (p *Parser) create() (UpdateCmd, error) {
	// CREATE
	if err := p.lex.EatKeyword("create"); err != nil {
		return nil, err
	}

	if p.lex.MatchKeyword("table") {
		// <CreateTable>
		return p.CreateTable()
	} else if p.lex.MatchKeyword("view") {
		// <CreateView>
		return p.CreateView()
	} else {
		// <CreateIndex>
		return p.CreateIndex()
	}
}

// DELETE文の構文解析

// <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
func (p *Parser) Delete() (*DeleteData, error) {
	// DELETE
	if err := p.lex.EatKeyword("delete"); err != nil {
		return nil, err
	}

	// FROM
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}

	// IdTok
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// [ WHERE <Predicate> ]
	pred, err := p.whereOpt()
	if err != nil {
		return nil, err
	}

	return NewDeleteData(tableName, pred), nil
}

// INSERT文の構文解析

// <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
func (p *Parser) Insert() (*InsertData, error) {
	// INSERT
	if err := p.lex.EatKeyword("insert"); err != nil {
		return nil, err
	}

	// INTO
	if err := p.lex.EatKeyword("into"); err != nil {
		return nil, err
	}

	// IdTok
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// (
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	// <FieldList>
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}

	// )
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	// VALUES
	if err := p.lex.EatKeyword("values"); err != nil {
		return nil, err
	}

	// (
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	// <ConstList>
	values, err := p.constList()
	if err != nil {
		return nil, err
	}

	// )
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewInsertData(tableName, fields, values), nil
}

// <FieldList> := <Field> [ , <FieldList> ]
func (p *Parser) fieldList() ([]string, error) {
	// <Field>
	field, err := p.Field()
	if err != nil {
		return nil, err
	}

	fields := []string{field}

	// [ , <FieldList> ]
	if p.lex.MatchDelim(',') {
		// ,
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		// <FieldList>
		rest, err := p.fieldList()
		if err != nil {
			return nil, err
		}

		fields = append(fields, rest...)
	}

	return fields, nil
}

// <ConstList> := <Constant> [ , <ConstList> ]
func (p *Parser) constList() ([]*query.Constant, error) {
	// <Constant>
	value, err := p.Constant()
	if err != nil {
		return nil, err
	}

	values := []*query.Constant{value}

	// [ , <ConstList> ]
	if p.lex.MatchDelim(',') {
		// ,
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		// <ConstList>
		rest, err := p.constList()
		if err != nil {
			return nil, err
		}

		values = append(values, rest...)
	}

	return values, nil
}

// UPDATE文の構文解析

// <Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
func (p *Parser) Modify() (*ModifyData, error) {
	// UPDATE
	if err := p.lex.EatKeyword("update"); err != nil {
		return nil, err
	}

	// IdTok
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// SET
	if err := p.lex.EatKeyword("set"); err != nil {
		return nil, err
	}

	// <Field>
	fieldName, err := p.Field()
	if err != nil {
		return nil, err
	}

	// =
	if err := p.lex.EatDelim('='); err != nil {
		return nil, err
	}

	// <Expression>
	newValue, err := p.Expression()
	if err != nil {
		return nil, err
	}

	// [ WHERE <Predicate> ]
	pred, err := p.whereOpt()
	if err != nil {
		return nil, err
	}

	return NewModifyData(tableName, fieldName, newValue, pred), nil
}

// CREATE TABLE文の構文解析

// <CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
func (p *Parser) CreateTable() (*CreateTableData, error) {
	// TABLE
	if err := p.lex.EatKeyword("table"); err != nil {
		return nil, err
	}

	// IdTok
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// (
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	// <FieldDefs>
	schema, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}

	// )
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewCreateTableData(tableName, schema), nil
}

// <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
func (p *Parser) fieldDefs() (*record.Schema, error) {
	// <FieldDef>
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}

	// [ , <FieldDefs> ]
	if p.lex.MatchDelim(',') {
		// ,
		if err := p.lex.EatDelim(','); err != nil {
			return nil, err
		}

		// <FieldDefs>
		rest, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}

		schema.AddAll(rest)
	}

	return schema, nil
}

// <FieldDef> := IdTok <TypeDef>
func (p *Parser) fieldDef() (*record.Schema, error) {
	// IdTok
	fieldName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// <TypeDef>
	schema, err := p.fieldType(fieldName)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// <TypeDef> := INT | VARCHAR ( IntTok )
func (p *Parser) fieldType(fieldName string) (*record.Schema, error) {
	schema := record.NewSchema()

	if p.lex.MatchKeyword("int") {
		// INT
		if err := p.lex.EatKeyword("int"); err != nil {
			return nil, err
		}

		schema.AddIntField(fieldName)
	} else {
		// VARCHAR
		if err := p.lex.EatKeyword("varchar"); err != nil {
			return nil, err
		}

		// (
		if err := p.lex.EatDelim('('); err != nil {
			return nil, err
		}

		// IntTok
		strLen, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}

		// )
		if err := p.lex.EatDelim(')'); err != nil {
			return nil, err
		}

		schema.AddStringField(fieldName, strLen)
	}

	return schema, nil
}

// CREATE VIEW文の構文解析

// <CreateView> := CREATE VIEW IdTok AS <Query>
func (p *Parser) CreateView() (*CreateViewData, error) {
	// VIEW
	if err := p.lex.EatKeyword("view"); err != nil {
		return nil, err
	}

	// IdTok
	viewName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// AS
	if err := p.lex.EatKeyword("as"); err != nil {
		return nil, err
	}

	// <Query>
	query, err := p.Query()
	if err != nil {
		return nil, err
	}

	return NewCreateViewData(viewName, query), nil
}

// CREATE INDEX文の構文解析

// <CreateIndex> := CREATE INDEX IdTok ON IdTok ( <Field> )
func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	// INDEX
	if err := p.lex.EatKeyword("index"); err != nil {
		return nil, err
	}

	// IdTok
	indexName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// ON
	if err := p.lex.EatKeyword("on"); err != nil {
		return nil, err
	}

	// IdTok
	tableName, err := p.lex.EatIdentifier()
	if err != nil {
		return nil, err
	}

	// (
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	// <Field>
	fieldName, err := p.Field()
	if err != nil {
		return nil, err
	}

	// )
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	return NewCreateIndexData(indexName, tableName, fieldName), nil
}
