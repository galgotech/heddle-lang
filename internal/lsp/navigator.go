package lsp

import (
	"strings"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// Navigator provides methods to traverse the AST based on source locations.
type Navigator struct {
	ctx *ast.ASTContext
}

// DefinitionAt returns the range of the definition for the symbol at the given position.
func (n *Navigator) DefinitionAt(program ast.ProgramNode, line, col uint32) *ast.Range {
	name, symType := n.SymbolAt(program, line, col)
	if name == "" {
		return nil
	}

	// Check for local assignments in the active workflow/handler containing (line, col) first
	// Check Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		wfRef := n.ctx.WorkflowRefs[i]
		wfRange := n.ctx.WorkflowRanges[wfRef]
		if n.inRange(wfRange, line, col) {
			wf := n.ctx.WorkflowNodes[wfRef]
			for j := wf.StatementRefsStart; j < wf.StatementRefsEnd; j++ {
				stmtRef := n.ctx.StatementRefs[j]
				stmt := n.ctx.PipelineStatementNodes[stmtRef]
				if n.ctx.GetString(stmt.AssignmentRef) == name {
					r := n.ctx.AssignmentRanges[stmtRef]
					return &r
				}
			}
		}
	}
	// Check Handlers
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		hRef := n.ctx.HandlerRefs[i]
		hRange := n.ctx.HandlerRanges[hRef]
		if n.inRange(hRange, line, col) {
			h := n.ctx.HandlerNodes[hRef]
			for j := h.HandlerStatementRefsStart; j < h.HandlerStatementRefsEnd; j++ {
				hsRef := n.ctx.HandlerStatementRefs[j]
				hs := n.ctx.HandlerStatementNodes[hsRef]
				stmt := n.ctx.PipelineStatementNodes[hs.StmtRef]
				if n.ctx.GetString(stmt.AssignmentRef) == name {
					r := n.ctx.AssignmentRanges[hs.StmtRef]
					return &r
				}
			}
		}
	}

	// If we are already on a definition, return it
	if symType == "resource" || symType == "step" || symType == "workflow" || symType == "handler" {
		// To be more precise, we could find the specific node, but n.SymbolAt already checks ranges.
		// Let's re-find it to get the range.
	}

	// Find the definition range by name
	// Imports
	for i := program.ImportRefsStart; i < program.ImportRefsEnd; i++ {
		ref := n.ctx.ImportRefs[i]
		if n.ctx.GetString(n.ctx.ImportNodes[ref].AliasRef) == name {
			r := n.ctx.ImportRanges[ref]
			return &r
		}
	}
	// Resources
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		if n.ctx.GetString(n.ctx.ResourceNodes[ref].NameRef) == name {
			r := n.ctx.ResourceNameRanges[ref]
			return &r
		}
	}
	// Steps
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		if n.ctx.GetString(n.ctx.StepBindingNodes[ref].NameRef) == name {
			r := n.ctx.StepNameRanges[ref]
			return &r
		}
	}
	// Handlers
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		if n.ctx.GetString(n.ctx.HandlerNodes[ref].NameRef) == name {
			r := n.ctx.HandlerNameRanges[ref]
			return &r
		}
	}
	// Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		if n.ctx.GetString(n.ctx.WorkflowNodes[ref].NameRef) == name {
			r := n.ctx.WorkflowNameRanges[ref]
			return &r
		}
	}

	return nil
}

// SymbolInfo represents information about a top-level symbol.
type SymbolInfo struct {
	Name  string
	Kind  string // resource, step, handler, workflow
	Range ast.Range
}

// DocumentSymbols returns a list of all top-level symbols in the program.
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

func (n *Navigator) resolveImportNamespace(program ast.ProgramNode, alias string) string {
	for i := program.ImportRefsStart; i < program.ImportRefsEnd; i++ {
		ref := n.ctx.ImportRefs[i]
		node := n.ctx.ImportNodes[ref]
		if n.ctx.GetString(node.AliasRef) == alias {
			path := n.ctx.GetString(node.PathRef)
			path = strings.Trim(path, "\"")
			parts := strings.Split(path, "/")
			if len(parts) > 0 {
				return parts[len(parts)-1]
			}
		}
	}
	return alias
}

// SymbolAt returns the name of the symbol at the given position and its type.
func (n *Navigator) SymbolAt(program ast.ProgramNode, line, col uint32) (string, string) {
	// Check Imports (aliases)
	for i := program.ImportRefsStart; i < program.ImportRefsEnd; i++ {
		ref := n.ctx.ImportRefs[i]
		r := n.ctx.ImportRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.ImportNodes[ref]
			return n.ctx.GetString(node.AliasRef), "import"
		}
	}

	// Check Assignments (> identifier)
	for i := 0; i < len(n.ctx.PipelineStatementNodes); i++ {
		ref := ast.NodeRef(i)
		r := n.ctx.AssignmentRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.PipelineStatementNodes[ref]
			return n.ctx.GetString(node.AssignmentRef), "assignment"
		}
	}

	// Check all FunctionRefNodes (handles step definitions, qualified calls, and resource mappings)
	for i := 1; i < len(n.ctx.FunctionRefNodes); i++ {
		frRef := ast.NodeRef(i)
		frNode := n.ctx.FunctionRefNodes[frRef]

		// Check resource mappings inside <key=value>
		if frNode.ResourcesRefRef != 0 {
			rr := n.ctx.ResourceRefNodes[frNode.ResourcesRefRef]
			for j := rr.MappingsRefStart; j < rr.MappingsRefEnd; j++ {
				mappingRef := n.ctx.MappingRefs[j]
				mapping := n.ctx.ResourceMappingNodes[mappingRef]
				if mapping.ValueRange.Start != mapping.ValueRange.End && n.inRange(mapping.ValueRange, line, col) {
					return n.ctx.GetString(mapping.ValueRef), "resource"
				}
			}
		}

		// Check module prefix first (if cursor is on the import alias prefix)
		modRange := n.ctx.FunctionRefModuleRanges[frRef]
		if modRange.Start != modRange.End && n.inRange(modRange, line, col) {
			return n.ctx.GetString(frNode.ModuleRef), "import"
		}

		// Check step name
		nameRange := n.ctx.FunctionRefNameRanges[frRef]
		if nameRange.Start != nameRange.End && n.inRange(nameRange, line, col) {
			name := n.ctx.GetString(frNode.NameRef)
			if frNode.ModuleRef.Start != frNode.ModuleRef.End {
				alias := n.ctx.GetString(frNode.ModuleRef)
				namespace := n.resolveImportNamespace(program, alias)
				return namespace + "." + name, "reference"
			}
			return name, "reference"
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
				if node.TrapRef.Start != node.TrapRef.End {
					return n.ctx.GetString(node.TrapRef), "reference"
				}
			}
		}
	}

	// Check Resources
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		r := n.ctx.ResourceNameRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.ResourceNodes[ref]
			return n.ctx.GetString(node.NameRef), "resource"
		}
	}

	// Check Steps
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		r := n.ctx.StepNameRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.StepBindingNodes[ref]
			return n.ctx.GetString(node.NameRef), "step"
		}
	}

	// Check Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		r := n.ctx.WorkflowNameRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.WorkflowNodes[ref]
			return n.ctx.GetString(node.NameRef), "workflow"
		}
	}

	// Check Handlers
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		r := n.ctx.HandlerNameRanges[ref]
		if n.inRange(r, line, col) {
			node := n.ctx.HandlerNodes[ref]
			return n.ctx.GetString(node.NameRef), "handler"
		}
	}

	return "", ""
}

// FindAllOccurrences returns all source ranges where the given symbol is used.
func (n *Navigator) FindAllOccurrences(program ast.ProgramNode, symbolName string, symbolType string, line, col uint32) []ast.Range {
	ranges := []ast.Range{}

	// If it's a reference, try to resolve it to "assignment" or "step/resource/workflow/handler"
	if symbolType == "reference" {
		// Find workflow containing (line, col)
		for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
			wfRef := n.ctx.WorkflowRefs[i]
			wfRange := n.ctx.WorkflowRanges[wfRef]
			if n.inRange(wfRange, line, col) {
				wf := n.ctx.WorkflowNodes[wfRef]
				// Look for assignment before this position
				foundAssignment := false
				for j := wf.StatementRefsStart; j < wf.StatementRefsEnd; j++ {
					stmtRef := n.ctx.StatementRefs[j]
					stmt := n.ctx.PipelineStatementNodes[stmtRef]
					stmtRange := n.ctx.AssignmentRanges[stmtRef]
					if n.ctx.GetString(stmt.AssignmentRef) == symbolName {
						// Assignment is available for subsequent statements.
						// If this statement ends before our position, it might be the definition.
						if stmtRange.End.Line < line || (stmtRange.End.Line == line && stmtRange.End.Col < col) {
							foundAssignment = true
							break
						}
					}
				}
				if foundAssignment {
					symbolType = "assignment"
				} else {
					symbolType = "step" // Or resource/workflow/handler, but they all share global scope
				}
				break
			}
		}
		if symbolType == "reference" {
			symbolType = "step"
		}
	}

	if symbolType == "assignment" {
		// Local to workflow
		for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
			wfRef := n.ctx.WorkflowRefs[i]
			wfRange := n.ctx.WorkflowRanges[wfRef]
			if n.inRange(wfRange, line, col) {
				wf := n.ctx.WorkflowNodes[wfRef]
				foundDef := false
				for j := wf.StatementRefsStart; j < wf.StatementRefsEnd; j++ {
					stmtRef := n.ctx.StatementRefs[j]
					stmt := n.ctx.PipelineStatementNodes[stmtRef]
					stmtRange := n.ctx.AssignmentRanges[stmtRef]

					isThisDef := false
					if n.ctx.GetString(stmt.AssignmentRef) == symbolName {
						if (stmtRange.Start.Line == line && stmtRange.Start.Col <= col && stmtRange.End.Col >= col) || foundDef {
							ranges = append(ranges, stmtRange)
							foundDef = true
							isThisDef = true
						}
					}

					if foundDef && !isThisDef {
						n.addCallUsages(stmt.ExprRef, symbolName, &ranges)
					}
				}
				return ranges
			}
		}
	}

	// Default: Global symbols
	// Definitions
	// Resources
	for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
		ref := n.ctx.ResourceRefs[i]
		node := n.ctx.ResourceNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.ResourceNameRanges[ref])
		}
	}
	// Steps
	for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
		ref := n.ctx.StepRefs[i]
		node := n.ctx.StepBindingNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.StepNameRanges[ref])
		}
	}
	// Handlers
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		ref := n.ctx.HandlerRefs[i]
		node := n.ctx.HandlerNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.HandlerNameRanges[ref])
		}
	}
	// Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		node := n.ctx.WorkflowNodes[ref]
		if n.ctx.GetString(node.NameRef) == symbolName {
			ranges = append(ranges, n.ctx.WorkflowNameRanges[ref])
		}
	}
	// Imports
	for i := program.ImportRefsStart; i < program.ImportRefsEnd; i++ {
		ref := n.ctx.ImportRefs[i]
		node := n.ctx.ImportNodes[ref]
		if n.ctx.GetString(node.AliasRef) == symbolName {
			ranges = append(ranges, n.ctx.ImportRanges[ref])
		}
	}
	// Qualified calls module prefixes (import alias usages)
	for i := 1; i < len(n.ctx.FunctionRefNodes); i++ {
		ref := ast.NodeRef(i)
		node := n.ctx.FunctionRefNodes[ref]
		if n.ctx.GetString(node.ModuleRef) == symbolName {
			r := n.ctx.FunctionRefModuleRanges[ref]
			if r.Start != r.End {
				ranges = append(ranges, r)
			}
		}
	}

	// Usages (only those not shadowed)
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		wfRef := n.ctx.WorkflowRefs[i]
		wf := n.ctx.WorkflowNodes[wfRef]
		shadowed := false
		for j := wf.StatementRefsStart; j < wf.StatementRefsEnd; j++ {
			stmtRef := n.ctx.StatementRefs[j]
			stmt := n.ctx.PipelineStatementNodes[stmtRef]

			if !shadowed {
				n.addCallUsages(stmt.ExprRef, symbolName, &ranges)
			}

			if n.ctx.GetString(stmt.AssignmentRef) == symbolName {
				shadowed = true
			}
		}
	}

	// Also check calls outside workflows (e.g. in handlers)
	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		hRef := n.ctx.HandlerRefs[i]
		h := n.ctx.HandlerNodes[hRef]
		for j := h.HandlerStatementRefsStart; j < h.HandlerStatementRefsEnd; j++ {
			hsRef := n.ctx.HandlerStatementRefs[j]
			hs := n.ctx.HandlerStatementNodes[hsRef]
			stmt := n.ctx.PipelineStatementNodes[hs.StmtRef]
			n.addCallUsages(stmt.ExprRef, symbolName, &ranges)
		}
	}

	return ranges
}

func (n *Navigator) addCallUsages(exprRef ast.NodeRef, symbolName string, ranges *[]ast.Range) {
	if exprRef == 0 {
		return
	}

	// Check if it's a PipeChainNode
	for i := 0; i < len(n.ctx.PipeChainNodes); i++ {
		pRef := ast.NodeRef(i)
		if pRef == exprRef {
			pc := n.ctx.PipeChainNodes[pRef]
			for k := pc.CallRefsStart; k < pc.CallRefsEnd; k++ {
				cRef := n.ctx.CallRefs[k]
				call := n.ctx.CallNodes[cRef]
				if !call.IsPrql {
					if n.ctx.GetString(call.NameRef) == symbolName {
						*ranges = append(*ranges, n.ctx.CallRanges[cRef])
					}
					if n.ctx.GetString(call.TrapRef) == symbolName {
						// This is slightly wrong as CallRanges covers the whole call,
						// but without specific TrapRanges it's the best we can do.
						*ranges = append(*ranges, n.ctx.CallRanges[cRef])
					}
					if call.FunctionRef != 0 {
						frNode := n.ctx.FunctionRefNodes[call.FunctionRef]
						if n.ctx.GetString(frNode.NameRef) == symbolName {
							r := n.ctx.FunctionRefNameRanges[call.FunctionRef]
							if r.Start != r.End {
								*ranges = append(*ranges, r)
							}
						}
					}
				}
			}
			return
		}
	}
}

// SelectionRanges returns selection ranges.
func (n *Navigator) SelectionRanges(program ast.ProgramNode, line, col uint32) []ast.Range {
	ranges := []ast.Range{}
	// Check Workflows
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		ref := n.ctx.WorkflowRefs[i]
		if n.inRange(n.ctx.WorkflowRanges[ref], line, col) {
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

// NewNavigator creates a new Navigator.
func NewNavigator(ctx *ast.ASTContext) *Navigator {
	return &Navigator{ctx: ctx}
}
