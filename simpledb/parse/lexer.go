package parse

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// lexer 今回作るLexerのインターフェースを説明するための型
type lexer interface {
	// 次のトークンが何かを調べるメソッド郡
	MatchDelim(d rune) bool
	MatchIntConstant() bool
	MatchStringConstant() bool
	MatchKeyword(word string) bool
	MatchIdentifier() bool

	// トークンを読み進めるメソッド郡
	// 一致しないトークンを読み進めようとした場合 BadSyntaxError を返す
	EatDelim(d rune) error
	EatIntConstant() (int32, error)
	EatStringConstant() (string, error)
	EatKeyword(word string) error
	EatIdentifier() (string, error)
}

type tokenKind int

const (
	tokenKindEOF tokenKind = iota + 1
	tokenKindDelimiter
	tokenKindInteger
	tokenKindString
	tokenKindKeyword
	tokenKindIdentifier
)

type token struct {
	kind  tokenKind
	value string
}

// 空白文字
const whiteSpaces string = " \t\r\n"

// 予約語
var keywords = map[string]struct{}{
	"select":  {},
	"from":    {},
	"where":   {},
	"and":     {},
	"insert":  {},
	"into":    {},
	"values":  {},
	"delete":  {},
	"update":  {},
	"set":     {},
	"create":  {},
	"table":   {},
	"int":     {},
	"varchar": {},
	"view":    {},
	"as":      {},
	"index":   {},
	"on":      {},
}

var _ lexer = (*Lexer)(nil)

type Lexer struct {
	input       string
	token       *token
	whiteSpaces string
	keywords    map[string]struct{}
}

func NewLexer(
	input string,
) (*Lexer, error) {
	l := &Lexer{
		input:       input,
		token:       nil,
		whiteSpaces: whiteSpaces,
		keywords:    keywords,
	}

	if err := l.nextToken(); err != nil {
		return nil, err
	}

	return l, nil
}

// MatchDelim 現在のトークンが指定されたデリミタか
func (l *Lexer) MatchDelim(d rune) bool {
	return l.token.kind == tokenKindDelimiter && l.token.value == string(d)
}

// MatchIntConstant 現在のトークンが整数か
func (l *Lexer) MatchIntConstant() bool {
	return l.token.kind == tokenKindInteger
}

// MatchStringConstant 現在のトークンが文字列リテラルか
func (l *Lexer) MatchStringConstant() bool {
	return l.token.kind == tokenKindString
}

// MatchStringConstant 現在のトークンが指定されたキーワードか
func (l *Lexer) MatchKeyword(word string) bool {
	return l.token.kind == tokenKindKeyword && l.token.value == word
}

// MatchIdentifier 現在のトークンが識別子か
func (l *Lexer) MatchIdentifier() bool {
	return l.token.kind == tokenKindIdentifier
}

// EatDelim 現在のトークンが指定されたデリミタであれば次のトークンを読み進める
func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return NewBadSyntaxError(fmt.Sprintf("expected %c, but got %q", d, l.token.value))
	}

	if err := l.nextToken(); err != nil {
		return err
	}

	return nil
}

// EatIntConstant 現在のトークンが整数であれば次のトークンを読み進める
func (l *Lexer) EatIntConstant() (int32, error) {
	if !l.MatchIntConstant() {
		return 0, NewBadSyntaxError(fmt.Sprintf("expected integer, but got %q", l.token.value))
	}

	value, err := strconv.Atoi(l.token.value)
	if err != nil {
		return 0, err
	}

	if err := l.nextToken(); err != nil {
		return 0, err
	}

	return int32(value), nil
}

// EatStringConstant 現在のトークンが文字列リテラルであれば次のトークンを読み進める
func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", NewBadSyntaxError(fmt.Sprintf("expected string, but got %q", l.token.value))
	}

	value := l.token.value

	if err := l.nextToken(); err != nil {
		return "", err
	}

	return value, nil
}

// EatKeyword 現在のトークンが指定されたキーワードであれば次のトークンを読み進める
func (l *Lexer) EatKeyword(word string) error {
	if !l.MatchKeyword(word) {
		return NewBadSyntaxError(fmt.Sprintf("expected %q, but got %q", word, l.token.value))
	}

	if err := l.nextToken(); err != nil {
		return err
	}

	return nil
}

// EatIdentifier 現在のトークンが識別子であれば次のトークンを読み進める
func (l *Lexer) EatIdentifier() (string, error) {
	if !l.MatchIdentifier() {
		return "", NewBadSyntaxError(fmt.Sprintf("expected identifier, but got %q", l.token.value))
	}

	value := l.token.value

	if err := l.nextToken(); err != nil {
		return "", err
	}

	return value, nil
}

// nextToken トークンを1つ読み進める
// MEMO: 元の実装では TokenizerStream を使っているが、自前実装する
func (l *Lexer) nextToken() error {
	// 空白文字を捨てる
	l.input = strings.TrimLeft(l.input, l.whiteSpaces)

	if len(l.input) == 0 {
		l.token = &token{
			kind:  tokenKindEOF,
			value: "",
		}
		return nil
	}

	switch r := l.input[0]; {
	case isDigit(r):
		return l.readInteger()
	case r == '\'':
		return l.readString()
	case isIdentifierStart(r):
		return l.readIdentifier()
	default:
		return l.readDelimiter()
	}
}

// Integer: [0-9]+
func (l *Lexer) readInteger() error {
	// [0-9]
	pos := 1

	// [0-9]*
	for ; pos < len(l.input); pos++ {
		if !isDigit(l.input[pos]) {
			break
		}
	}

	l.token = &token{
		kind:  tokenKindInteger,
		value: l.input[:pos],
	}

	l.input = l.input[pos:]
	return nil
}

// String: `'` .* `'`
func (l *Lexer) readString() error {
	// `'`
	pos := 1

	// .*
	close := strings.IndexByte(l.input[pos:], '\'')
	if close == -1 {
		return NewBadSyntaxError("unterminated string")
	}

	pos += close

	content := l.input[1:pos]

	// `'`
	pos += 1

	l.token = &token{
		kind:  tokenKindString,
		value: content,
	}

	l.input = l.input[pos:]
	return nil
}

// Identifier: [A-Z_a-z][0-9A-Z_a-z]*
func (l *Lexer) readIdentifier() error {
	// [A-Z_a-z]
	pos := 1

	// [0-9A-Z_a-z]*
	for ; pos < len(l.input); pos++ {
		if !isIdentifierContinuation(l.input[pos]) {
			break
		}
	}

	word := strings.ToLower(l.input[0:pos]) // 小文字に変換する

	kind := tokenKindIdentifier
	if _, ok := l.keywords[word]; ok {
		kind = tokenKindKeyword
	}

	l.token = &token{
		kind:  kind,
		value: word,
	}

	l.input = l.input[pos:]
	return nil
}

// Delimiter: .
// MEMO: 2文字以上の演算子 (`<>`) などは今回実装しない
func (l *Lexer) readDelimiter() error {
	// .
	_, size := utf8.DecodeRuneInString(l.input)

	l.token = &token{
		kind:  tokenKindDelimiter,
		value: l.input[:size],
	}

	l.input = l.input[size:]
	return nil
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isIdentifierStart(c byte) bool {
	return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || c == '_'
}

func isIdentifierContinuation(c byte) bool {
	return isIdentifierStart(c) || isDigit(c)
}
