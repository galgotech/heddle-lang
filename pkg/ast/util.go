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

// GetRange returns the range of a node.
func GetRange(node Node) Range {
	if node == nil {
		return Range{}
	}

	// Some nodes might have their own range calculation in the future.
	// For now, we use the start token and calculate end based on length.
	var startTok lexer.Token
	var endTok lexer.Token

	switch n := node.(type) {
	case *Identifier:
		startTok = n.Token
		endTok = n.Token
	case *StringLiteral:
		startTok = n.Token
		endTok = n.Token
	case *NumberLiteral:
		startTok = n.Token
		endTok = n.Token
	case *BooleanLiteral:
		startTok = n.Token
		endTok = n.Token
	case *NullLiteral:
		startTok = n.Token
		endTok = n.Token
	case *ResourceBinding:
		startTok = n.Token
		if n.Ref != nil {
			return Range{
				Start: Position{Line: n.Token.Line, Column: n.Token.Column},
				End:   GetRange(n.Ref).End,
			}
		}
		endTok = n.Name.Token
	case *StepBinding:
		startTok = n.Token
		if n.Ref != nil {
			return Range{
				Start: Position{Line: n.Token.Line, Column: n.Token.Column},
				End:   GetRange(n.Ref).End,
			}
		}
		endTok = n.Name.Token
	case *FunctionRef:
		if n.Module != nil {
			startTok = n.Module.Token
		} else {
			startTok = n.Name.Token
		}
		endTok = n.Name.Token
	case *SchemaRef:
		if n.Module != nil {
			startTok = n.Module.Token
		} else {
			startTok = n.Name.Token
		}
		endTok = n.Name.Token
	case *StepCall:
		startTok = n.Token
		endTok = n.Name.Token
	default:
		return Range{}
	}

	return Range{
		Start: Position{Line: startTok.Line, Column: startTok.Column},
		End:   Position{Line: endTok.Line, Column: endTok.Column + len(endTok.Literal)},
	}
}

// isWithin checks if the given line/column is within the range.
func isWithin(r Range, line, column int) bool {
	if r.Start.Line == 0 {
		return false
	}
	if line < r.Start.Line || line > r.End.Line {
		return false
	}
	if line == r.Start.Line && column < r.Start.Column {
		return false
	}
	if line == r.End.Line && column > r.End.Column {
		return false
	}
	return true
}

// FindNodeAt traverses the AST to find the deepest node that contains the given position.
func FindNodeAt(program *Program, line, column int) Node {
	if program == nil {
		return nil
	}
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

	// Special check for typed nils which are common in ASTs
	switch n := node.(type) {
	case *Identifier:
		if n == nil {
			return nil
		}
	case *FunctionRef:
		if n == nil {
			return nil
		}
	case *SchemaRef:
		if n == nil {
			return nil
		}
	case *StepCall:
		if n == nil {
			return nil
		}
	}

	// 1. Check children first (to find the deepest node)
	switch n := node.(type) {
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

	// 2. Check the node itself if no children matched
	if isWithin(GetRange(node), line, column) {
		return node
	}

	return nil
}
