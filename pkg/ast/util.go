package ast

import (
	"github.com/galgotech/heddle-lang/pkg/lexer"
)

// Position represents a line and column in the source code.
type Position struct {
	Line   int
	Column int
}

// Range represents a start and end position in the source code.
type Range struct {
	Start Position
	End   Position
}

// GetRange returns the range of a node based on its token and literal length.
func GetRange(node Node) Range {
	// This is a simplified implementation. Some nodes might span multiple lines.
	// For now, we use the start token.
	var tok lexer.Token
	switch n := node.(type) {
	case *Identifier:
		tok = n.Token
	case *StringLiteral:
		tok = n.Token
	case *NumberLiteral:
		tok = n.Token
	case *BooleanLiteral:
		tok = n.Token
	case *NullLiteral:
		tok = n.Token
	case *StepCall:
		tok = n.Token
	case *SchemaRef:
		if n.Module != nil {
			tok = n.Module.Token
		} else {
			tok = n.Name.Token
		}
	default:
		// Fallback to TokenLiteral if we can't get the token directly
		// but this is not very reliable for range calculation.
		return Range{}
	}

	return Range{
		Start: Position{Line: tok.Line, Column: tok.Column},
		End:   Position{Line: tok.Line, Column: tok.Column + len(tok.Literal)},
	}
}

// FindNodeAt traverses the AST to find the deepest node that contains the given position.
func FindNodeAt(program *Program, line, column int) Node {
	for _, stmt := range program.Statements {
		if node := findInNode(stmt, line, column); node != nil {
			return node
		}
	}
	return nil
}

func findInNode(node Node, line, column int) Node {
	if node == nil {
		return nil
	}

	// Check if the position is within the node's range
	// For complex nodes, we first check their children.
	
	switch n := node.(type) {
	case *Program:
		for _, s := range n.Statements {
			if found := findInNode(s, line, column); found != nil {
				return found
			}
		}
	case *ImportStatement:
		if found := findInNode(n.Path, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Alias, line, column); found != nil {
			return found
		}
	case *SchemaDefinition:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Block, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Ref, line, column); found != nil {
			return found
		}
	case *ResourceBinding:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Ref, line, column); found != nil {
			return found
		}
	case *StepBinding:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Signature, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Ref, line, column); found != nil {
			return found
		}
	case *HandlerDefinition:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
		for _, s := range n.Statements {
			if found := findInNode(s, line, column); found != nil {
				return found
			}
		}
	case *WorkflowDefinition:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
		if found := findInNode(n.TrapHandler, line, column); found != nil {
			return found
		}
		for _, s := range n.Statements {
			if found := findInNode(s, line, column); found != nil {
				return found
			}
		}
	case *PipelineStatement:
		if found := findInNode(n.Expression, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Assignment, line, column); found != nil {
			return found
		}
	case *PipeChain:
		for _, c := range n.Calls {
			if found := findInNode(c, line, column); found != nil {
				return found
			}
		}
	case *CallExpression:
		if found := findInNode(n.Step, line, column); found != nil {
			return found
		}
		if found := findInNode(n.TrapHandler, line, column); found != nil {
			return found
		}
	case *StepCall:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
	case *TrapHandler:
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
	case *StepSignature:
		if found := findInNode(n.Input, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Output, line, column); found != nil {
			return found
		}
	case *FunctionRef:
		if found := findInNode(n.Module, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
	case *SchemaRef:
		if found := findInNode(n.Module, line, column); found != nil {
			return found
		}
		if found := findInNode(n.Name, line, column); found != nil {
			return found
		}
	}

	// If it's a leaf node or none of the children matched, check the node itself.
	r := GetRange(node)
	if line == r.Start.Line && column >= r.Start.Column && column <= r.End.Column {
		return node
	}

	return nil
}
