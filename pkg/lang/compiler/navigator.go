package compiler

import (
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// Navigator provides methods to traverse the AST based on source locations.
type Navigator struct {
	ctx *ast.ASTContext
}

// NewNavigator creates a new Navigator.
func NewNavigator(ctx *ast.ASTContext) *Navigator {
	return &Navigator{ctx: ctx}
}

// DefinitionAt returns the range of the definition for the symbol at the given position.
func (n *Navigator) DefinitionAt(program ast.ProgramNode, line, col uint32) *ast.Range {
	name, symType := n.SymbolAt(program, line, col)
	if name == "" {
		return nil
	}

	// If we are already on a definition, return it
	if symType == "resource" || symType == "step" || symType == "workflow" || symType == "handler" {
		// To be more precise, we could find the specific node, but n.SymbolAt already checks ranges.
		// Let's re-find it to get the range.
	}

	// Find the definition range by name
	// Resources
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		if n.ctx.GetString(n.ctx.ResourceNodes[ref].NameRef) == name {
			r := n.ctx.ResourceRanges[ref]
			return &r
		}
	}
	// Steps
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		if n.ctx.GetString(n.ctx.StepBindingNodes[ref].NameRef) == name {
			r := n.ctx.StepRanges[ref]
			return &r
		}
	}
	// Handlers
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		if n.ctx.GetString(n.ctx.HandlerNodes[ref].NameRef) == name {
			r := n.ctx.HandlerRanges[ref]
			return &r
		}
	}
	// Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		if n.ctx.GetString(n.ctx.WorkflowNodes[ref].NameRef) == name {
			r := n.ctx.WorkflowRanges[ref]
			return &r
		}
	}

	return nil
}

// DocumentSymbols returns a list of all top-level symbols in the program.
type SymbolInfo struct {
	Name  string
	Kind  string // resource, step, handler, workflow
	Range ast.Range
}

func (n *Navigator) DocumentSymbols(program ast.ProgramNode) []SymbolInfo {
	symbols := []SymbolInfo{}

	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		symbols = append(symbols, SymbolInfo{
			Name:  n.ctx.GetString(n.ctx.ResourceNodes[ref].NameRef),
			Kind:  "resource",
			Range: n.ctx.ResourceRanges[ref],
		})
	}
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		symbols = append(symbols, SymbolInfo{
			Name:  n.ctx.GetString(n.ctx.StepBindingNodes[ref].NameRef),
			Kind:  "step",
			Range: n.ctx.StepRanges[ref],
		})
	}
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		symbols = append(symbols, SymbolInfo{
			Name:  n.ctx.GetString(n.ctx.HandlerNodes[ref].NameRef),
			Kind:  "handler",
			Range: n.ctx.HandlerRanges[ref],
		})
	}
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		symbols = append(symbols, SymbolInfo{
			Name:  n.ctx.GetString(n.ctx.WorkflowNodes[ref].NameRef),
			Kind:  "workflow",
			Range: n.ctx.WorkflowRanges[ref],
		})
	}

	return symbols
}

// SymbolAt returns the name of the symbol at the given position and its type.
func (n *Navigator) SymbolAt(program ast.ProgramNode, line, col uint32) (string, string) {
	// Check Resources
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		r := n.ctx.ResourceRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.ResourceNodes[ref]
			return n.ctx.GetString(node.NameRef), "resource"
		}
	}

	// Check Steps
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		r := n.ctx.StepRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.StepBindingNodes[ref]
			return n.ctx.GetString(node.NameRef), "step"
		}
	}

	// Check Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		r := n.ctx.WorkflowRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.WorkflowNodes[ref]
			return n.ctx.GetString(node.NameRef), "workflow"
		}
	}

	// Check Calls (references)
	for i := 0; i < len(n.ctx.CallNodes); i++ {
		ref := ast.NodeRef(i)
		r := n.ctx.CallRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.CallNodes[ref]
			if !node.IsPrql {
				if node.NameRef.Start != node.NameRef.End {
					return n.ctx.GetString(node.NameRef), "reference"
				}
			}
		}
	}

	return "", ""
}

// FindAllOccurrences returns all source ranges where the given symbol is used.
func (n *Navigator) FindAllOccurrences(program ast.ProgramNode, symbolName string) []ast.Range {
	ranges := []ast.Range{}

	// Definitions
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		node := n.ctx.ResourceNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.ResourceRanges[ref])
		}
	}
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		node := n.ctx.StepBindingNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.StepRanges[ref])
		}
	}
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		node := n.ctx.HandlerNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.HandlerRanges[ref])
		}
	}
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		node := n.ctx.WorkflowNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.WorkflowRanges[ref])
		}
	}

	// Usages
	for i := 0; i < len(n.ctx.CallNodes); i++ {
		ref := ast.NodeRef(i)
		node := n.ctx.CallNodes[ref]
		if !node.IsPrql && n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.CallRanges[ref])
		}
	}

	return ranges
}

func (n *Navigator) SelectionRanges(program ast.ProgramNode, line, col uint32) []ast.Range {
	ranges := []ast.Range{}
	// This is a simplified version. A real implementation would visit the AST nodes.
	// But our ASTContext doesn't have a generic visitor easily.
	// We'll check the main blocks.

	// Check Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		if n.inRange(n.ctx.WorkflowRanges[ref], line, col) {
			wd := n.ctx.WorkflowNodes[ref]
			_ = wd // For future use
			ranges = append(ranges, n.ctx.WorkflowRanges[ref])
		}
	}

	return ranges
}

func (n *Navigator) inRange(r ast.Range, line, col uint32) bool {
	if line < r.Start.Line || line > r.End.Line {
		return false
	}
	if line == r.Start.Line && col < r.Start.Col {
		return false
	}
	if line == r.End.Line && col > r.End.Col {
		return false
	}
	return true
}
