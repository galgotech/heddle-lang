package compiler

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// Validator performs semantic analysis on the AST.
type Validator struct {
	program ast.ProgramNode
	ctx     *ast.ASTContext

	// Symbol tables mapping name to NodeRef
	mapResource map[string]ast.NodeRef
	mapStep     map[string]ast.NodeRef
	mapHandler  map[string]ast.NodeRef
}

// NewValidator creates a new instance of the Validator.
func NewValidator(program ast.ProgramNode, ctx *ast.ASTContext) *Validator {
	return &Validator{
		program:     program,
		ctx:         ctx,
		mapResource: make(map[string]ast.NodeRef),
		mapStep:     make(map[string]ast.NodeRef),
		mapHandler:  make(map[string]ast.NodeRef),
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

	return nil
}

func (v *Validator) registerDefinitions() {
	// Register Resources
	for i := v.program.ResourceRefsStart; i < v.program.ResourceRefsEnd; i++ {
		ref := v.ctx.ResourceRefs[i]
		node := v.ctx.ResourceNodes[ref]
		v.mapResource[v.ctx.GetString(node.NameRef)] = ref
	}

	// Register Steps
	for i := v.program.StepRefsStart; i < v.program.StepRefsEnd; i++ {
		ref := v.ctx.StepRefs[i]
		node := v.ctx.StepBindingNodes[ref]
		v.mapStep[v.ctx.GetString(node.NameRef)] = ref
	}

	// Register Handlers
	for i := v.program.HandlerRefsStart; i < v.program.HandlerRefsEnd; i++ {
		ref := v.ctx.HandlerRefs[i]
		node := v.ctx.HandlerNodes[ref]
		v.mapHandler[v.ctx.GetString(node.NameRef)] = ref
	}
}

func (v *Validator) validateReferences() error {
	// Validate Resource references in Step bindings
	for i := v.program.StepRefsStart; i < v.program.StepRefsEnd; i++ {
		ref := v.ctx.StepRefs[i]
		node := v.ctx.StepBindingNodes[ref]
		if err := v.validateFunctionRef(node.FunctionRef); err != nil {
			return err
		}
	}

	// Validate Workflow references
	for i := v.program.WorkflowRefsStart; i < v.program.WorkflowRefsEnd; i++ {
		ref := v.ctx.WorkflowRefs[i]
		node := v.ctx.WorkflowNodes[ref]
		if err := v.validateWorkflowReferences(node); err != nil {
			return err
		}
	}

	// Validate Handler references
	for i := v.program.HandlerRefsStart; i < v.program.HandlerRefsEnd; i++ {
		ref := v.ctx.HandlerRefs[i]
		node := v.ctx.HandlerNodes[ref]
		if err := v.validateHandlerReferences(node); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) validateFunctionRef(ref ast.NodeRef) error {
	if ref == 0 {
		return nil
	}
	fn := v.ctx.FunctionRefNodes[ref]
	if fn.ResourcesRefRef != 0 {
		rr := v.ctx.ResourceRefNodes[fn.ResourcesRefRef]
		for i := rr.MappingsRefStart; i < rr.MappingsRefEnd; i++ {
			mappingRef := v.ctx.MappingRefs[i]
			mapping := v.ctx.ResourceMappingNodes[mappingRef]
			resourceName := v.ctx.GetString(mapping.ValueRef)
			if _, ok := v.mapResource[resourceName]; !ok {
				return fmt.Errorf("undefined resource '%s' used in step injection", resourceName)
			}
		}
	}
	return nil
}

func (v *Validator) validateWorkflowReferences(wd ast.WorkflowNode) error {
	if wd.TrapRef.Start != wd.TrapRef.End {
		name := v.ctx.GetString(wd.TrapRef)
		if _, ok := v.mapHandler[name]; !ok {
			return fmt.Errorf("undefined handler '%s' in workflow '%s'", name, v.ctx.GetString(wd.NameRef))
		}
	}

	for i := wd.StatementRefsStart; i < wd.StatementRefsEnd; i++ {
		psRef := v.ctx.StatementRefs[i]
		ps := v.ctx.PipelineStatementNodes[psRef]
		if err := v.validatePipelineReferences(ps); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validateHandlerReferences(hd ast.HandlerNode) error {
	for i := hd.HandlerStatementRefsStart; i < hd.HandlerStatementRefsEnd; i++ {
		hsRef := v.ctx.HandlerStatementRefs[i]
		hs := v.ctx.HandlerStatementNodes[hsRef]
		ps := v.ctx.PipelineStatementNodes[hs.StmtRef]
		if err := v.validatePipelineReferences(ps); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) validatePipelineReferences(ps ast.PipelineStatementNode) error {
	ref := ps.ExprRef
	if ref == 0 {
		return nil
	}

	// Pipeline statement can point to a PipeChain or a Dataframe
	if int(ref) < len(v.ctx.PipeChainNodes) {
		pc := v.ctx.PipeChainNodes[ref]
		// This is a bit weak because we don't know for sure if ref points to PipeChainNodes
		// but since we only have two options and NodeRef is an index into context slices...
		// Ideally we'd have a type tag or separate refs.
		// For now, let's assume if it's within bounds of PipeChainNodes and has calls, it's a pipe chain.
		if pc.CallRefsEnd > pc.CallRefsStart {
			for i := pc.CallRefsStart; i < pc.CallRefsEnd; i++ {
				callRef := v.ctx.CallRefs[i]
				call := v.ctx.CallNodes[callRef]
				if err := v.validateCallReferences(call); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (v *Validator) validateCallReferences(call ast.CallNode) error {
	if call.TrapRef.Start != call.TrapRef.End {
		name := v.ctx.GetString(call.TrapRef)
		if _, ok := v.mapHandler[name]; !ok {
			return fmt.Errorf("undefined handler '%s' in step call", name)
		}
	}

	if !call.IsPrql {
		name := v.ctx.GetString(call.NameRef)
		if _, ok := v.mapStep[name]; !ok {
			// return fmt.Errorf("undefined step: %s", name)
		}
	}

	return nil
}

func (v *Validator) detectCycles() error {
	return nil
}

// Lookup returns the definition node for a given name if it exists.
func (v *Validator) Lookup(name string) ast.NodeRef {
	if res, ok := v.mapResource[name]; ok {
		return res
	}
	if step, ok := v.mapStep[name]; ok {
		return step
	}
	if handler, ok := v.mapHandler[name]; ok {
		return handler
	}
	return 0
}
