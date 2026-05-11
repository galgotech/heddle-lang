package analyzer

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
)

// Severity represents the diagnostic level.
type Severity int

const (
	Error Severity = iota
	Warning
	Information
	Hint
)

// Diagnostic represents a semantic or syntactic issue in the code.
type Diagnostic struct {
	Message  string
	Range    ast.Range
	Severity Severity
	Help     string
}

// Analyzer performs semantic validation on the Heddle AST.
type Analyzer struct {
	ctx      *ast.ASTContext
	registry *locality.DataLocalityRegistry
	errors   []Diagnostic
}

// New initializes a new Analyzer with the given AST context and locality registry.
func New(ctx *ast.ASTContext, registry *locality.DataLocalityRegistry) *Analyzer {
	return &Analyzer{
		ctx:      ctx,
		registry: registry,
	}
}

// Analyze performs a full semantic pass over the AST.
func (a *Analyzer) Analyze(program ast.ProgramNode) []Diagnostic {
	a.errors = nil

	a.validateDAG(program)
	a.validateLocality(program)

	return a.errors
}

func (a *Analyzer) validateDAG(program ast.ProgramNode) {
	// Detect circular dependencies in workflows.
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		wfRef := a.ctx.WorkflowRefs[i]
		wf := a.ctx.WorkflowNodes[wfRef]

		visited := make(map[string]bool)
		a.checkWorkflowCycles(wf, visited)
	}
}

func (a *Analyzer) checkWorkflowCycles(wf ast.WorkflowNode, visited map[string]bool) {
	// Simple cycle detection: if a step calls a workflow that eventually calls this workflow.
}

func (a *Analyzer) validateLocality(program ast.ProgramNode) {
	// Iterate through all workflows and handlers to check if called steps exist in the registry.
	for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
		wfRef := a.ctx.WorkflowRefs[i]
		wf := a.ctx.WorkflowNodes[wfRef]
		for j := wf.StatementRefsStart; j < wf.StatementRefsEnd; j++ {
			stmtRef := a.ctx.StatementRefs[j]
			stmt := a.ctx.PipelineStatementNodes[stmtRef]
			a.validatePipeChain(stmt.ExprRef)
		}
	}

	for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
		hRef := a.ctx.HandlerRefs[i]
		h := a.ctx.HandlerNodes[hRef]
		for j := h.HandlerStatementRefsStart; j < h.HandlerStatementRefsEnd; j++ {
			hsRef := a.ctx.HandlerStatementRefs[j]
			hs := a.ctx.HandlerStatementNodes[hsRef]
			stmt := a.ctx.PipelineStatementNodes[hs.StmtRef]
			a.validatePipeChain(stmt.ExprRef)
		}
	}
}

func (a *Analyzer) validatePipeChain(ref ast.NodeRef) {
	if ref == 0 {
		return
	}
	// For now assume it's a PipeChain if it's within bounds.
	if int(ref) < len(a.ctx.PipeChainNodes) {
		chain := a.ctx.PipeChainNodes[ref]
		for i := chain.CallRefsStart; i < chain.CallRefsEnd; i++ {
			callRef := a.ctx.CallRefs[i]
			call := a.ctx.CallNodes[callRef]
			if call.IsPrql {
				continue
			}
			name := a.ctx.GetString(call.NameRef)

			// Check if the step is defined in StepBindings or available in the registry.
			if !a.isStepDefined(name) {
				a.reportError(a.ctx.CallRanges[callRef],
					fmt.Sprintf("Step '%s' is not defined in the current context.", name),
					fmt.Sprintf("Did you forget to import it or define it with 'step %s: ...'?", name))
			}
		}
	}
}

func (a *Analyzer) isStepDefined(name string) bool {
	// First check StepBindings in the AST.
	for _, sb := range a.ctx.StepBindingNodes {
		if a.ctx.GetString(sb.NameRef) == name {
			return true
		}
	}

	// Then check the DataLocalityRegistry.
	if a.registry != nil {
		_, ok := a.registry.GetData(name)
		return ok
	}

	return false
}

func (a *Analyzer) reportError(r ast.Range, msg, help string) {
	a.errors = append(a.errors, Diagnostic{
		Message:  msg,
		Range:    r,
		Severity: Error,
		Help:     help,
	})
}
