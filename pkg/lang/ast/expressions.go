package ast

import (
	"bytes"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

// PipeChain represents a sequence of steps connected by pipes.
type PipeChain struct {
	Calls []*CallExpression
}

func (pc *PipeChain) TokenLiteral() string {
	if len(pc.Calls) > 0 {
		return pc.Calls[0].TokenLiteral()
	}
	return ""
}
func (pc *PipeChain) String() string {
	var out bytes.Buffer
	calls := []string{}
	for _, c := range pc.Calls {
		calls = append(calls, c.String())
	}
	out.WriteString(strings.Join(calls, " | "))
	return out.String()
}
func (pc *PipeChain) expressionNode() {}

// CallExpression represents an invocation of a step with an optional trap handler.
type CallExpression struct {
	Step        Expression  // StepCall or AnonymousStepExpression
	TrapHandler *TrapHandler // Optional: ?Handler
}

func (ce *CallExpression) TokenLiteral() string { return ce.Step.TokenLiteral() }
func (ce *CallExpression) String() string {
	var out bytes.Buffer
	out.WriteString(ce.Step.String())
	if ce.TrapHandler != nil {
		out.WriteString(ce.TrapHandler.String())
	}
	return out.String()
}
func (ce *CallExpression) expressionNode() {}

// StepCall represents a call to a named step.
type StepCall struct {
	Token lexer.Token // the lexer.IDENT token
	Name  *Identifier
}

func (sc *StepCall) TokenLiteral() string { return sc.Token.Literal }
func (sc *StepCall) String() string       { return sc.Name.String() }
func (sc *StepCall) expressionNode()      {}

// AnonymousStepExpression represents an inline step definition.
type AnonymousStepExpression struct {
	Signature *StepSignature
	Ref       Expression // FunctionRef or PRQLExpression
}

func (ase *AnonymousStepExpression) TokenLiteral() string { return ase.Signature.TokenLiteral() }
func (ase *AnonymousStepExpression) String() string {
	var out bytes.Buffer
	out.WriteString(ase.Signature.String())
	out.WriteString(" = ")
	out.WriteString(ase.Ref.String())
	return out.String()
}
func (ase *AnonymousStepExpression) expressionNode() {}

// PRQLExpression represents an inline PRQL query block.
type PRQLExpression struct {
	Token lexer.Token // the lexer.PRQL_BLOCK token
	Value string
}

func (pe *PRQLExpression) TokenLiteral() string { return pe.Token.Literal }
func (pe *PRQLExpression) String() string       { return "(" + pe.Value + ")" }
func (pe *PRQLExpression) expressionNode()      {}

// TrapHandler represents a "?" handler reference.
type TrapHandler struct {
	Token lexer.Token // the lexer.QUESTION token
	Name  *Identifier
}

func (th *TrapHandler) TokenLiteral() string { return th.Token.Literal }
func (th *TrapHandler) String() string {
	return "?" + th.Name.String()
}
func (th *TrapHandler) expressionNode() {}

// StepSignature represents the type contract of a step.
type StepSignature struct {
	Input  Node // SchemaRef or VoidType
	Output Node // SchemaRef or VoidType
}

func (ss *StepSignature) TokenLiteral() string { return ss.Input.TokenLiteral() }
func (ss *StepSignature) String() string {
	return ss.Input.String() + " -> " + ss.Output.String()
}
func (ss *StepSignature) expressionNode() {}

// VoidType represents the lack of a schema.
type VoidType struct {
	Token lexer.Token // the lexer.VOID token
}

func (vt *VoidType) TokenLiteral() string { return vt.Token.Literal }
func (vt *VoidType) String() string       { return "void" }

// FunctionRef represents a reference to a host function.
type FunctionRef struct {
	Module   *Identifier
	Name     *Identifier
	Resource map[string]string   // Optional: <res=val>
	Config   *Dictionary         // Optional: {cfg:val}
}

func (fr *FunctionRef) TokenLiteral() string { return fr.Module.TokenLiteral() }
func (fr *FunctionRef) String() string {
	var out bytes.Buffer
	out.WriteString(fr.Module.String())
	out.WriteString(".")
	out.WriteString(fr.Name.String())
	// Skipping resource/config for brevity in String()
	return out.String()
}
func (fr *FunctionRef) expressionNode() {}

// SchemaRef represents a reference to a schema.
type SchemaRef struct {
	Module *Identifier // Optional
	Name   *Identifier
}

func (sr *SchemaRef) TokenLiteral() string {
	if sr.Module != nil {
		return sr.Module.TokenLiteral()
	}
	return sr.Name.TokenLiteral()
}
func (sr *SchemaRef) String() string {
	if sr.Module != nil {
		return sr.Module.String() + "." + sr.Name.String()
	}
	return sr.Name.String()
}
func (sr *SchemaRef) expressionNode() {}

// Dataframe represents a collection of data rows.
type Dataframe struct {
	Token lexer.Token // the lexer.LBRACKET token
	Rows  []*Dictionary
}

func (df *Dataframe) TokenLiteral() string { return df.Token.Literal }
func (df *Dataframe) String() string {
	var out bytes.Buffer
	out.WriteString("[")
	// Simplified
	out.WriteString(" ... ")
	out.WriteString("]")
	return out.String()
}
func (df *Dataframe) expressionNode() {}
