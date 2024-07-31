package parse

// PredParser 単純な式の再帰下降パーサ。構文が正しいかどうかの判定のみで、パース結果を返さない。
// 実際には使用しない
type PredParser struct {
	lex *Lexer
}

func NewPredParser(input string) (*PredParser, error) {
	lex, err := NewLexer(input)
	if err != nil {
		return nil, err
	}

	return &PredParser{
		lex: lex,
	}, nil
}

// <Field> := IdTok
func (p *PredParser) Field() error {
	// IdTok
	if _, err := p.lex.EatIdentifier(); err != nil {
		return err
	}

	return nil
}

// <Constant> := StrTok | IntTok
func (p *PredParser) Constant() error {
	if p.lex.MatchStringConstant() {
		// StrTok
		if _, err := p.lex.EatStringConstant(); err != nil {
			return err
		}
	} else {
		// IntTok
		if _, err := p.lex.EatIntConstant(); err != nil {
			return err
		}
	}

	return nil
}

// <Expression> := <Field> | <Constant>
func (p *PredParser) Expression() error {
	if p.lex.MatchIdentifier() {
		// <Field>
		if err := p.Field(); err != nil {
			return err
		}
	} else {
		// Constant
		if err := p.Constant(); err != nil {
			return err
		}
	}

	return nil
}

// <Term> := <Expression> = <Expression>
func (p *PredParser) Term() error {
	// <Expression>
	if err := p.Expression(); err != nil {
		return err
	}

	// =
	if err := p.lex.EatDelim('='); err != nil {
		return err
	}

	// <Expression>
	if err := p.Expression(); err != nil {
		return err
	}

	return nil
}

// <Predicate> := <Term> [ AND <Predicate> ]
func (p *PredParser) Predicate() error {
	// <Term>
	if err := p.Term(); err != nil {
		return err
	}

	// [ AND <Predicate> ]
	// MEMO: 右結合になる
	if p.lex.MatchKeyword("and") {
		// AND
		if err := p.lex.EatKeyword("and"); err != nil {
			return err
		}

		// <Predicate>
		if err := p.Predicate(); err != nil {
			return err
		}
	}

	return nil
}
