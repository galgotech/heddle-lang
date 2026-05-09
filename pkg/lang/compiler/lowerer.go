package compiler

import (
	"github.com/google/uuid"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

// Lowerer orchestrates the transformation of the Abstract Syntax Tree (AST) into
// the Intermediate Representation (IR). It manages symbol resolution and
// instruction generation across multiple lowering passes.
type Lowerer struct {
	ctx     *ast.ASTContext // The source AST context containing all nodes.
	program *ir.ProgramIR   // The target IR program being constructed.

	// Symbol resolution maps for cross-referencing declarations.
	mapImport   map[string]string      // Maps module aliases to their absolute paths.
	mapResource map[string]ast.NodeRef // Maps resource names to their AST node references.
	mapStep     map[string]ast.NodeRef // Maps step binding names to their AST node references.
	mapHandler  map[string]ast.NodeRef // Maps error handler names to their AST node references.
	handlerIDs  map[string]string      // Maps handler names to their generated IR IDs for trap resolution.
}

// Lower translates the provided AST ProgramNode into a ProgramIR.
// It executes four distinct passes to ensure all cross-references (traps, resources)
// are correctly resolved across the workflow topology.
func (l *Lowerer) Lower(astProgram ast.ProgramNode) (*ir.ProgramIR, error) {
	// Pass 1: Symbol Registration. Populates lookup tables for all top-level definitions.
	for i := astProgram.ImportRefsStart; i < astProgram.ImportRefsEnd; i++ {
		ref := l.ctx.ImportRefs[i]
		node := l.ctx.ImportNodes[ref]
		alias := l.ctx.GetString(node.AliasRef)
		if alias != "" {
			l.mapImport[alias] = l.ctx.GetString(node.PathRef)
		}
	}

	for i := astProgram.ResourceRefsStart; i < astProgram.ResourceRefsEnd; i++ {
		ref := l.ctx.ResourceRefs[i]
		node := l.ctx.ResourceNodes[ref]
		l.mapResource[l.ctx.GetString(node.NameRef)] = ref
	}

	for i := astProgram.StepRefsStart; i < astProgram.StepRefsEnd; i++ {
		ref := l.ctx.StepRefs[i]
		node := l.ctx.StepBindingNodes[ref]
		l.mapStep[l.ctx.GetString(node.NameRef)] = ref
	}

	for i := astProgram.HandlerRefsStart; i < astProgram.HandlerRefsEnd; i++ {
		ref := l.ctx.HandlerRefs[i]
		node := l.ctx.HandlerNodes[ref]
		l.mapHandler[l.ctx.GetString(node.NameRef)] = ref
	}

	// Pass 2: Resource Lowering. Transforms stateful resource declarations into IR.
	for _, ref := range l.mapResource {
		node := l.ctx.ResourceNodes[ref]
		res, err := l.lowerResource(node)
		if err != nil {
			return nil, err
		}
		l.registerInstruction(res)
	}

	// Pass 3: Handler Lowering. Translates error handlers into FlowInstructions.
	// Handler IDs are pre-allocated to allow steps to reference them via traps.
	for i := astProgram.HandlerRefsStart; i < astProgram.HandlerRefsEnd; i++ {
		ref := l.ctx.HandlerRefs[i]
		node := l.ctx.HandlerNodes[ref]
		name := l.ctx.GetString(node.NameRef)
		l.handlerIDs[name] = uuid.New().String()
	}

	for i := astProgram.HandlerRefsStart; i < astProgram.HandlerRefsEnd; i++ {
		ref := l.ctx.HandlerRefs[i]
		node := l.ctx.HandlerNodes[ref]
		flow, err := l.lowerHandler(node)
		if err != nil {
			return nil, err
		}
		l.registerInstruction(flow)
	}

	// Pass 4: Workflow Lowering. Translates main workflow blocks into execution entry points.
	for i := astProgram.WorkflowRefsStart; i < astProgram.WorkflowRefsEnd; i++ {
		ref := l.ctx.WorkflowRefs[i]
		node := l.ctx.WorkflowNodes[ref]
		flow, err := l.lowerWorkflow(node)
		if err != nil {
			return nil, err
		}
		l.registerInstruction(flow)
		l.program.Workflows = append(l.program.Workflows, flow.ID)
	}

	return l.program, nil
}

// lowerResource transforms an AST resource node into a ResourceInstruction.
func (l *Lowerer) lowerResource(astResource ast.ResourceNode) (*ir.ResourceInstruction, error) {
	res := &ir.ResourceInstruction{
		BaseInstruction: l.newBase(ir.ResourceInst),
		Name:            l.ctx.GetString(astResource.NameRef),
	}
	return res, nil
}

// lowerHandler lowers a handler block into a FlowInstruction (DAG).
func (l *Lowerer) lowerHandler(astHandler ast.HandlerNode) (*ir.FlowInstruction, error) {
	name := l.ctx.GetString(astHandler.NameRef)
	flow := &ir.FlowInstruction{
		BaseInstruction: ir.BaseInstruction{
			ID:   l.handlerIDs[name],
			Type: ir.FlowInst,
		},
		Name: name,
	}

	for i := astHandler.HandlerStatementRefsStart; i < astHandler.HandlerStatementRefsEnd; i++ {
		hsRef := l.ctx.HandlerStatementRefs[i]
		hs := l.ctx.HandlerStatementNodes[hsRef]
		ps := l.ctx.PipelineStatementNodes[hs.StmtRef]
		headID, err := l.lowerPipeline(ps)
		if err != nil {
			return nil, err
		}
		flow.Heads = append(flow.Heads, headID)
	}

	return flow, nil
}

// lowerWorkflow lowers a main workflow block into a FlowInstruction.
func (l *Lowerer) lowerWorkflow(astWorkflow ast.WorkflowNode) (*ir.FlowInstruction, error) {
	flow := &ir.FlowInstruction{
		BaseInstruction: l.newBase(ir.FlowInst),
		Name:            l.ctx.GetString(astWorkflow.NameRef),
	}

	// Resolve the workflow-level error handler if specified via the trap operator '?'.
	if astWorkflow.TrapRef.Start != astWorkflow.TrapRef.End {
		handlerName := l.ctx.GetString(astWorkflow.TrapRef)
		if id, ok := l.handlerIDs[handlerName]; ok {
			flow.Handler = id
		}
	}

	for i := astWorkflow.StatementRefsStart; i < astWorkflow.StatementRefsEnd; i++ {
		psRef := l.ctx.StatementRefs[i]
		ps := l.ctx.PipelineStatementNodes[psRef]
		headID, err := l.lowerPipeline(ps)
		if err != nil {
			return nil, err
		}
		flow.Heads = append(flow.Heads, headID)
	}

	return flow, nil
}

// lowerPipeline dispatches a pipeline statement to its specific lowering logic.
func (l *Lowerer) lowerPipeline(ps ast.PipelineStatementNode) (string, error) {
	ref := ps.ExprRef
	if ref == 0 {
		return "", nil
	}

	// For now assume it's a PipeChain if it's within bounds.
	// In a real compiler we'd have a more robust way to distinguish Expr types.
	if int(ref) < len(l.ctx.PipeChainNodes) {
		pc := l.ctx.PipeChainNodes[ref]
		return l.lowerPipeChain(pc, l.ctx.GetString(ps.AssignmentRef))
	}

	return "", nil
}

// lowerPipeChain connects a sequence of calls into a linked list of IR instructions.
func (l *Lowerer) lowerPipeChain(pc ast.PipeChainNode, assignment string) (string, error) {
	var prevStep *ir.StepInstruction
	var firstStepID string

	callCount := pc.CallRefsEnd - pc.CallRefsStart

	for i := uint32(0); i < callCount; i++ {
		callRef := l.ctx.CallRefs[pc.CallRefsStart+i]
		call := l.ctx.CallNodes[callRef]
		step, err := l.lowerCall(call)
		if err != nil {
			return "", err
		}

		if i == 0 {
			firstStepID = step.ID
		}

		// Connect steps in a pipeline to form an execution chain.
		if prevStep != nil {
			prevStep.Next = step.ID
		}

		// Apply variable assignment to the final step in the pipeline.
		if i == callCount-1 && assignment != "" {
			step.Assignment = assignment
		}

		l.registerInstruction(step)
		prevStep = step
	}

	return firstStepID, nil
}

// lowerCall transforms an AST call (standard or PRQL) into a StepInstruction.
func (l *Lowerer) lowerCall(call ast.CallNode) (*ir.StepInstruction, error) {
	step := &ir.StepInstruction{
		BaseInstruction: l.newBase(ir.StepInst),
		Resources:       make(map[string]string),
		Config:          make(map[string]any),
	}

	if call.IsPrql {
		step.DefinitionName = "prql"
		step.Config["query"] = l.ctx.GetString(call.QueryRef)
	} else {
		name := l.ctx.GetString(call.NameRef)
		step.DefinitionName = name

		// Resolve function binding and module mapping.
		if stepRef, ok := l.mapStep[name]; ok {
			binding := l.ctx.StepBindingNodes[stepRef]
			fn := l.ctx.FunctionRefNodes[binding.RefRef]

			step.Call = []string{
				l.ctx.GetString(fn.ModuleRef),
				l.ctx.GetString(fn.NameRef),
			}

			// Map resource requirements to the step configuration.
			if fn.ResourcesRef != 0 {
				rr := l.ctx.ResourceRefNodes[fn.ResourcesRef]
				for i := rr.MappingsRefStart; i < rr.MappingsRefEnd; i++ {
					mappingRef := l.ctx.MappingRefs[i]
					mapping := l.ctx.ResourceMappingNodes[mappingRef]
					step.Resources[l.ctx.GetString(mapping.KeyRef)] = l.ctx.GetString(mapping.ValueRef)
				}
			}
		}
	}

	// Resolve the step-level error handler if specified via the trap operator '?'.
	if call.TrapRef.Start != call.TrapRef.End {
		handlerName := l.ctx.GetString(call.TrapRef)
		if id, ok := l.handlerIDs[handlerName]; ok {
			step.Handler = id
		}
	}

	return step, nil
}

// registerInstruction adds a generated instruction to the IR program's registry.
func (l *Lowerer) registerInstruction(inst ir.Instruction) {
	l.program.Instructions[inst.GetID()] = inst
}

// newBase generates a new BaseInstruction with a unique ID and specified type.
func (l *Lowerer) newBase(t ir.InstructionType) ir.BaseInstruction {
	return ir.BaseInstruction{
		ID:   uuid.New().String(),
		Type: t,
	}
}

// NewLowerer creates a new instance of the Lowerer with a clean IR program state.
func NewLowerer(ctx *ast.ASTContext) *Lowerer {
	return &Lowerer{
		ctx: ctx,
		program: &ir.ProgramIR{
			BaseInstruction: ir.BaseInstruction{
				ID:   uuid.New().String(),
				Type: ir.ProgramInst,
			},
			Instructions: make(map[string]any),
			Workflows:    []string{},
		},
		mapImport:   make(map[string]string),
		mapResource: make(map[string]ast.NodeRef),
		mapStep:     make(map[string]ast.NodeRef),
		mapHandler:  make(map[string]ast.NodeRef),
		handlerIDs:  make(map[string]string),
	}
}
