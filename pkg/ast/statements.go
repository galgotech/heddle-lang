package ast

import (
	"bytes"

	"github.com/galgotech/heddle-lang/pkg/lexer"
)

// Declaration is a statement that defines a reusable component (schema, resource, step).
type Declaration interface {
	Statement
	declarationNode()
}

// Routine is a statement that defines an executable block (handler, workflow).
type Routine interface {
	Statement
	routineNode()
}

// ImportStatement represents an import of another Heddle module.
type ImportStatement struct {
	Token lexer.Token // the lexer.IMPORT token
	Path  *StringLiteral
	Alias *Identifier
}

func (is *ImportStatement) TokenLiteral() string { return is.Token.Literal }
func (is *ImportStatement) String() string {
	var out bytes.Buffer
	out.WriteString("import ")
	out.WriteString(is.Path.String())
	out.WriteString(" ")
	out.WriteString(is.Alias.String())
	return out.String()
}
func (is *ImportStatement) statementNode() {}

// SchemaDefinition represents a data schema definition.
type SchemaDefinition struct {
	Token lexer.Token // the lexer.SCHEMA token
	Name  *Identifier
	Block *SchemaBlock // Optional: if schema ID { ... }
	Ref   *SchemaRef   // Optional: if schema ID = Ref
}

func (sd *SchemaDefinition) TokenLiteral() string { return sd.Token.Literal }
func (sd *SchemaDefinition) String() string {
	var out bytes.Buffer
	out.WriteString("schema ")
	out.WriteString(sd.Name.String())
	if sd.Block != nil {
		out.WriteString(" ")
		out.WriteString(sd.Block.String())
	} else if sd.Ref != nil {
		out.WriteString(" = ")
		out.WriteString(sd.Ref.String())
	}
	return out.String()
}
func (sd *SchemaDefinition) statementNode()   {}
func (sd *SchemaDefinition) declarationNode() {}

// SchemaBlock represents a structured schema definition.
type SchemaBlock struct {
	Token  lexer.Token // the lexer.LBRACE token
	Fields map[string]interface{} // map[string](*SchemaBlock | string)
}

func (sb *SchemaBlock) TokenLiteral() string { return sb.Token.Literal }
func (sb *SchemaBlock) String() string {
	var out bytes.Buffer
	out.WriteString("{")
	// Simplified representation
	out.WriteString(" ... ")
	out.WriteString("}")
	return out.String()
}
func (sb *SchemaBlock) expressionNode() {}

// ResourceBinding represents a binding of a resource to a function.
type ResourceBinding struct {
	Token lexer.Token // the lexer.RESOURCE token
	Name  *Identifier
	Ref   *FunctionRef
}

func (rb *ResourceBinding) TokenLiteral() string { return rb.Token.Literal }
func (rb *ResourceBinding) String() string {
	var out bytes.Buffer
	out.WriteString("resource ")
	out.WriteString(rb.Name.String())
	out.WriteString(" = ")
	out.WriteString(rb.Ref.String())
	return out.String()
}
func (rb *ResourceBinding) statementNode()   {}
func (rb *ResourceBinding) declarationNode() {}

// StepBinding represents a binding of a step to a function with a signature.
type StepBinding struct {
	Token     lexer.Token // the lexer.STEP token
	Name      *Identifier
	Signature *StepSignature
	Ref       *FunctionRef
}

func (sb *StepBinding) TokenLiteral() string { return sb.Token.Literal }
func (sb *StepBinding) String() string {
	var out bytes.Buffer
	out.WriteString("step ")
	out.WriteString(sb.Name.String())
	out.WriteString(": ")
	out.WriteString(sb.Signature.String())
	out.WriteString(" = ")
	out.WriteString(sb.Ref.String())
	return out.String()
}
func (sb *StepBinding) statementNode()   {}
func (sb *StepBinding) declarationNode() {}

// HandlerDefinition represents an error or event handler.
type HandlerDefinition struct {
	Token      lexer.Token // the lexer.HANDLER token
	Name       *Identifier
	Statements []Statement // HandlerStatement: capture? pipeline
}

func (hd *HandlerDefinition) TokenLiteral() string { return hd.Token.Literal }
func (hd *HandlerDefinition) String() string {
	var out bytes.Buffer
	out.WriteString("handler ")
	out.WriteString(hd.Name.String())
	out.WriteString(" {")
	for _, s := range hd.Statements {
		out.WriteString(s.String())
	}
	out.WriteString("}")
	return out.String()
}
func (hd *HandlerDefinition) statementNode() {}
func (hd *HandlerDefinition) routineNode()   {}

// CaptureStatement represents the "*" capture in a handler.
type CaptureStatement struct {
	Token lexer.Token // the lexer.ASTERISK token
}

func (cs *CaptureStatement) TokenLiteral() string { return cs.Token.Literal }
func (cs *CaptureStatement) String() string       { return "*" }
func (cs *CaptureStatement) statementNode()       {}

// WorkflowDefinition represents a sequence of steps.
type WorkflowDefinition struct {
	Token       lexer.Token // the lexer.WORKFLOW token
	Name        *Identifier
	TrapHandler *Identifier // Optional: ?Handler
	Statements  []*PipelineStatement
}

func (wd *WorkflowDefinition) TokenLiteral() string { return wd.Token.Literal }
func (wd *WorkflowDefinition) String() string {
	var out bytes.Buffer
	out.WriteString("workflow ")
	out.WriteString(wd.Name.String())
	if wd.TrapHandler != nil {
		out.WriteString(" ?")
		out.WriteString(wd.TrapHandler.String())
	}
	out.WriteString(" {")
	for _, s := range wd.Statements {
		out.WriteString(s.String())
	}
	out.WriteString("}")
	return out.String()
}
func (wd *WorkflowDefinition) statementNode() {}
func (wd *WorkflowDefinition) routineNode()   {}

// PipelineStatement represents a dataframe or pipe chain, optionally assigned to a variable.
type PipelineStatement struct {
	Expression Expression // Dataframe or PipeChain
	Assignment *Identifier // Optional: > Var
}

func (ps *PipelineStatement) TokenLiteral() string { return ps.Expression.TokenLiteral() }
func (ps *PipelineStatement) String() string {
	var out bytes.Buffer
	out.WriteString(ps.Expression.String())
	if ps.Assignment != nil {
		out.WriteString(" > ")
		out.WriteString(ps.Assignment.String())
	}
	return out.String()
}
func (ps *PipelineStatement) statementNode() {}
