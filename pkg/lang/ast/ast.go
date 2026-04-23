package ast

import (
	"bytes"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

// Node is the base interface for all nodes in the Abstract Syntax Tree.
type Node interface {
	TokenLiteral() string
	String() string
}

// Statement represents a command or action that doesn't return a value.
type Statement interface {
	Node
	statementNode()
}

// Expression represents a construct that evaluates to a value.
type Expression interface {
	Node
	expressionNode()
}

// Program is the root node of every Heddle AST.
type Program struct {
	Statements []Statement
}

func (p *Program) TokenLiteral() string {
	if len(p.Statements) > 0 {
		return p.Statements[0].TokenLiteral()
	}
	return ""
}

func (p *Program) String() string {
	var out bytes.Buffer
	for _, s := range p.Statements {
		out.WriteString(s.String())
	}
	return out.String()
}

// Identifier represents a name in the code (e.g., variable, function, schema).
type Identifier struct {
	Token lexer.Token // the lexer.IDENT token
	Value string
}

func (i *Identifier) TokenLiteral() string { return i.Token.Literal }
func (i *Identifier) String() string       { return i.Value }
func (i *Identifier) expressionNode()      {}

// Literal represents a constant value.
type Literal interface {
	Expression
	literalNode()
}

// StringLiteral represents a string constant.
type StringLiteral struct {
	Token lexer.Token
	Value string
}

func (sl *StringLiteral) TokenLiteral() string { return sl.Token.Literal }
func (sl *StringLiteral) String() string       { return "\"" + sl.Value + "\"" }
func (sl *StringLiteral) expressionNode()      {}
func (sl *StringLiteral) literalNode()         {}

// NumberLiteral represents a numeric constant.
type NumberLiteral struct {
	Token lexer.Token
	Value float64
}

func (nl *NumberLiteral) TokenLiteral() string { return nl.Token.Literal }
func (nl *NumberLiteral) String() string       { return nl.Token.Literal }
func (nl *NumberLiteral) expressionNode()      {}
func (nl *NumberLiteral) literalNode()         {}

// BooleanLiteral represents true or false.
type BooleanLiteral struct {
	Token lexer.Token
	Value bool
}

func (bl *BooleanLiteral) TokenLiteral() string { return bl.Token.Literal }
func (bl *BooleanLiteral) String() string       { return bl.Token.Literal }
func (bl *BooleanLiteral) expressionNode()      {}
func (bl *BooleanLiteral) literalNode()         {}

// NullLiteral represents the null value.
type NullLiteral struct {
	Token lexer.Token
}

func (nl *NullLiteral) TokenLiteral() string { return nl.Token.Literal }
func (nl *NullLiteral) String() string       { return "null" }
func (nl *NullLiteral) expressionNode()      {}
func (nl *NullLiteral) literalNode()         {}

// Dictionary represents a key-value mapping.
type Dictionary struct {
	Token lexer.Token // the lexer.LBRACE token
	Pairs map[string]Expression
}

func (d *Dictionary) TokenLiteral() string { return d.Token.Literal }
func (d *Dictionary) String() string {
	var out bytes.Buffer
	pairs := []string{}
	for key, value := range d.Pairs {
		pairs = append(pairs, key+": "+value.String())
	}
	out.WriteString("{")
	out.WriteString(strings.Join(pairs, ", "))
	out.WriteString("}")
	return out.String()
}
func (d *Dictionary) expressionNode() {}
func (d *Dictionary) literalNode()    {}

// List represents an ordered collection of values.
type List struct {
	Token    lexer.Token // the lexer.LBRACKET token
	Elements []Expression
}

func (l *List) TokenLiteral() string { return l.Token.Literal }
func (l *List) String() string {
	var out bytes.Buffer
	elements := []string{}
	for _, e := range l.Elements {
		elements = append(elements, e.String())
	}
	out.WriteString("[")
	out.WriteString(strings.Join(elements, ", "))
	out.WriteString("]")
	return out.String()
}
func (l *List) expressionNode() {}
func (l *List) literalNode()    {}
