package compiler

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// Validator performs semantic analysis on the AST.
type Validator struct {
	program *ast.Program

	// Symbol tables
	mapResource map[string]*ast.ResourceBinding
	mapStep     map[string]*ast.StepBinding
	mapHandler  map[string]*ast.HandlerDefinition
}

// NewValidator creates a new instance of the Validator.
func NewValidator(program *ast.Program) *Validator {
	return &Validator{
		program:     program,
		mapResource: make(map[string]*ast.ResourceBinding),
		mapStep:     make(map[string]*ast.StepBinding),
		mapHandler:  make(map[string]*ast.HandlerDefinition),
	}
}

// Validate runs all semantic checks.
func (v *Validator) Validate() error {
	// 1. Register all definitions
	v.registerDefinitions()

	// 2. Validate references
	if err := v.validateReferences(); err != nil {
		return err
	}

	// 3. Detect cycles
	if err := v.detectCycles(); err != nil {
		return err
	}

	// 4. Validate types
	if err := v.validateTypes(); err != nil {
		return err
	}

	return nil
}

func (v *Validator) registerDefinitions() {
	for _, stmt := range v.program.Statements {
		switch s := stmt.(type) {
		case *ast.ResourceBinding:
			v.mapResource[s.Name.Value] = s
		case *ast.StepBinding:
			v.mapStep[s.Name.Value] = s
		case *ast.HandlerDefinition:
			v.mapHandler[s.Name.Value] = s
		}
	}
}

func (v *Validator) validateReferences() error {
	for _, stmt := range v.program.Statements {
		switch s := stmt.(type) {
		case *ast.WorkflowDefinition:
			if err := v.validateWorkflowReferences(s); err != nil {
				return err
			}
		case *ast.StepBinding:
			if err := v.validateStepReferences(s); err != nil {
				return err
			}
		case *ast.HandlerDefinition:
			if err := v.validateHandlerReferences(s); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) validateWorkflowReferences(wd *ast.WorkflowDefinition) error {
	if wd.TrapHandler != nil {
		if _, ok := v.mapHandler[wd.TrapHandler.Value]; !ok {
			return fmt.Errorf("undefined handler '%s' in workflow '%s'", wd.TrapHandler.Value, wd.Name.Value)
		}
	}

	for _, ps := range wd.Statements {
		if err := v.validatePipelineReferences(ps); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validatePipelineReferences(ps *ast.PipelineStatement) error {
	switch expr := ps.Expression.(type) {
	case *ast.PipeChain:
		for _, call := range expr.Calls {
			if err := v.validateCallReferences(call); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) validateCallReferences(call *ast.CallExpression) error {
	if call.TrapHandler != nil {
		if _, ok := v.mapHandler[call.TrapHandler.Name.Value]; !ok {
			return fmt.Errorf("undefined handler '%s' in step call", call.TrapHandler.Name.Value)
		}
	}

	switch s := call.Step.(type) {
	case *ast.StepCall:
		if _, ok := v.mapStep[s.Name.Value]; !ok {
			return fmt.Errorf("undefined step: %s", s.Name.Value)
		}
	case *ast.AnonymousStepExpression:
		if fr, ok := s.Ref.(*ast.FunctionRef); ok {
			for _, resName := range fr.Resource {
				if _, ok := v.mapResource[resName]; !ok {
					return fmt.Errorf("undefined resource: %s", resName)
				}
			}
		}
	}
	return nil
}

func (v *Validator) validateStepReferences(sb *ast.StepBinding) error {
	for _, resName := range sb.Ref.Resource {
		if _, ok := v.mapResource[resName]; !ok {
			return fmt.Errorf("undefined resource '%s' in step binding '%s'", resName, sb.Name.Value)
		}
	}
	return nil
}

func (v *Validator) validateHandlerReferences(hd *ast.HandlerDefinition) error {
	for _, stmt := range hd.Statements {
		if ps, ok := stmt.(*ast.PipelineStatement); ok {
			if err := v.validatePipelineReferences(ps); err != nil {
				return err
			}
		}
	}
	return nil
}

// Cycle Detection logic

func (v *Validator) detectCycles() error {
	// We check each workflow and each handler for cycles.
	// Since workflows/handlers can reference each other via trap handlers,
	// we need a global recursion stack.

	for _, stmt := range v.program.Statements {
		if wd, ok := stmt.(*ast.WorkflowDefinition); ok {
			visited := make(map[string]bool)
			recStack := make(map[string]bool)
			if err := v.checkWorkflowCycle(wd, visited, recStack); err != nil {
				return err
			}
		}
	}

	return nil
}

func (v *Validator) checkWorkflowCycle(wd *ast.WorkflowDefinition, visited, recStack map[string]bool) error {
	if recStack[wd.Name.Value] {
		return fmt.Errorf("cycle detected in workflow '%s'", wd.Name.Value)
	}
	if visited[wd.Name.Value] {
		return nil
	}

	visited[wd.Name.Value] = true
	recStack[wd.Name.Value] = true
	defer func() { recStack[wd.Name.Value] = false }()

	if wd.TrapHandler != nil {
		if err := v.checkHandlerCycle(v.mapHandler[wd.TrapHandler.Value], visited, recStack); err != nil {
			return err
		}
	}

	for _, ps := range wd.Statements {
		if err := v.checkPipelineCycle(ps, visited, recStack); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) checkHandlerCycle(hd *ast.HandlerDefinition, visited, recStack map[string]bool) error {
	key := "handler:" + hd.Name.Value
	if recStack[key] {
		return fmt.Errorf("cycle detected in handler '%s'", hd.Name.Value)
	}
	if visited[key] {
		return nil
	}

	visited[key] = true
	recStack[key] = true
	defer func() { recStack[key] = false }()

	for _, stmt := range hd.Statements {
		if ps, ok := stmt.(*ast.PipelineStatement); ok {
			if err := v.checkPipelineCycle(ps, visited, recStack); err != nil {
				return err
			}
		}
	}

	return nil
}

func (v *Validator) checkPipelineCycle(ps *ast.PipelineStatement, visited, recStack map[string]bool) error {
	switch expr := ps.Expression.(type) {
	case *ast.PipeChain:
		for _, call := range expr.Calls {
			if call.TrapHandler != nil {
				if err := v.checkHandlerCycle(v.mapHandler[call.TrapHandler.Name.Value], visited, recStack); err != nil {
					return err
				}
			}
			// Future: if steps could call other workflows, we'd add that check here.
		}
	}
	return nil
}

// Lookup returns the definition node for a given name if it exists.
func (v *Validator) Lookup(name string) ast.Node {
	if res, ok := v.mapResource[name]; ok {
		return res
	}
	if step, ok := v.mapStep[name]; ok {
		return step
	}
	if handler, ok := v.mapHandler[name]; ok {
		return handler
	}
	return nil
}

// Type Checking logic

func (v *Validator) validateTypes() error {
	for _, stmt := range v.program.Statements {
		switch s := stmt.(type) {
		case *ast.WorkflowDefinition:
			if err := v.validateWorkflowTypes(s); err != nil {
				return err
			}
		case *ast.HandlerDefinition:
			if err := v.validateHandlerTypes(s); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) validateWorkflowTypes(wd *ast.WorkflowDefinition) error {
	for _, ps := range wd.Statements {
		if err := v.validatePipelineTypes(ps); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validateHandlerTypes(hd *ast.HandlerDefinition) error {
	for _, stmt := range hd.Statements {
		if ps, ok := stmt.(*ast.PipelineStatement); ok {
			if err := v.validatePipelineTypes(ps); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) validatePipelineTypes(ps *ast.PipelineStatement) error {
	switch expr := ps.Expression.(type) {
	case *ast.PipeChain:
		var lastOutput ast.Node
		var lastStepName string

		for i, call := range expr.Calls {
			sig, err := v.getStepSignature(call.Step)
			if err != nil {
				return err
			}

			currentStepName := v.getStepName(call.Step)

			// 0. Check Trap Handler compatibility
			if call.TrapHandler != nil {
				handler := v.mapHandler[call.TrapHandler.Name.Value]
				if len(handler.Statements) > 0 {
					// Assume first statement in handler is a pipeline
					if ps, ok := handler.Statements[0].(*ast.PipelineStatement); ok {
						hSig, hErr := v.getPipelineFirstStepSignature(ps)
						if hErr == nil {
							if !v.areTypesCompatible(sig.Output, hSig.Input) {
								return fmt.Errorf("type mismatch in handler: step '%s' outputs '%s' but handler '%s' expects '%s'",
									currentStepName, sig.Output.String(), handler.Name.Value, hSig.Input.String())
							}
						}
					}
				}
			}

			// 1. Check if first step's input is void
			if i == 0 {
				if _, ok := sig.Input.(*ast.VoidType); !ok {
					// We currently only support pipelines starting with void input
					// unless they are explicitly fed by a resource/dataframe (not yet fully implemented in validation)
					// return fmt.Errorf("pipeline starting step '%s' must have 'void' input, got '%s'", currentStepName, sig.Input.String())
				}
			} else {
				// 2. Check compatibility with previous step
				if !v.areTypesCompatible(lastOutput, sig.Input) {
					return fmt.Errorf("type mismatch in pipe: step '%s' outputs '%s' but step '%s' expects '%s'",
						lastStepName, lastOutput.String(), currentStepName, sig.Input.String())
				}
			}

			// 3. Ensure no void -> void in intermediate stages
			if _, okIn := sig.Input.(*ast.VoidType); okIn && i > 0 {
				if _, okOut := sig.Output.(*ast.VoidType); okOut {
					return fmt.Errorf("invalid step '%s': 'void -> void' steps are not allowed in pipelines", currentStepName)
				}
			}

			lastOutput = sig.Output
			lastStepName = currentStepName
		}
	}
	return nil
}

func (v *Validator) getStepSignature(expr ast.Expression) (*ast.StepSignature, error) {
	switch e := expr.(type) {
	case *ast.StepCall:
		sb, ok := v.mapStep[e.Name.Value]
		if !ok {
			return nil, fmt.Errorf("undefined step: %s", e.Name.Value)
		}
		return sb.Signature, nil
	case *ast.AnonymousStepExpression:
		return e.Signature, nil
	default:
		return nil, fmt.Errorf("unsupported step expression type: %T", expr)
	}
}

func (v *Validator) getStepName(expr ast.Expression) string {
	switch e := expr.(type) {
	case *ast.StepCall:
		return e.Name.Value
	case *ast.AnonymousStepExpression:
		return "anonymous step"
	default:
		return "unknown"
	}
}

func (v *Validator) areTypesCompatible(out, in ast.Node) bool {
	if out == nil || in == nil {
		return false
	}

	// Exact match for void
	_, outVoid := out.(*ast.VoidType)
	_, inVoid := in.(*ast.VoidType)
	if outVoid && inVoid {
		return true
	}
	if outVoid || inVoid {
		return false
	}

	// Exact name match for schemas
	return out.String() == in.String()
}

func (v *Validator) getPipelineFirstStepSignature(ps *ast.PipelineStatement) (*ast.StepSignature, error) {
	switch expr := ps.Expression.(type) {
	case *ast.PipeChain:
		if len(expr.Calls) > 0 {
			return v.getStepSignature(expr.Calls[0].Step)
		}
	}
	return nil, fmt.Errorf("could not determine signature of pipeline")
}
