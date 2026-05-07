package compiler

import (
	"github.com/google/uuid"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
)

// Lowerer handles the translation of the Abstract Syntax Tree (AST) into
// the Intermediate Representation (IR).
type Lowerer struct {
	ctx     *ast.ASTContext
	program *ir.ProgramIR

	// Maps for resolving symbols during lowering
	mapImport   map[string]string // alias -> module path
	mapResource map[string]ast.NodeRef
	mapStep     map[string]ast.NodeRef
	mapHandler  map[string]ast.NodeRef
	handlerIDs  map[string]string // name -> IR ID
}

// NewLowerer creates a new instance of the Lowerer.
func NewLowerer(ctx *ast.ASTContext) *Lowerer {
	return &Lowerer{
		ctx: ctx,
		program: &ir.ProgramIR{
			BaseInstruction: ir.BaseInstruction{
				ID:   uuid.New().String(),
				Type: ir.ProgramInst,
			},
			Instructions: make(map[string]interface{}),
			Workflows:    []string{},
		},
		mapImport:   make(map[string]string),
		mapResource: make(map[string]ast.NodeRef),
		mapStep:     make(map[string]ast.NodeRef),
		mapHandler:  make(map[string]ast.NodeRef),
		handlerIDs:  make(map[string]string),
	}
}

// Lower translates an AST Program into a ProgramIR.
func (l *Lowerer) Lower(astProgram ast.ProgramNode) (*ir.ProgramIR, error) {
	// First pass: Register all definitions for resolution
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

	// Second pass: Lower resources
	for _, ref := range l.mapResource {
		node := l.ctx.ResourceNodes[ref]
		res, err := l.lowerResource(node)
		if err != nil {
			return nil, err
		}
		l.registerInstruction(res)
	}

	// Third pass: Lower handlers (collect IDs first for trap resolution)
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

	// Fourth pass: Lower workflows
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

func (l *Lowerer) lowerResource(astResource ast.ResourceNode) (*ir.ResourceInstruction, error) {
	res := &ir.ResourceInstruction{
		BaseInstruction: l.newBase(ir.ResourceInst),
		Name:            l.ctx.GetString(astResource.NameRef),
	}
	return res, nil
}

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

func (l *Lowerer) lowerWorkflow(astWorkflow ast.WorkflowNode) (*ir.FlowInstruction, error) {
	flow := &ir.FlowInstruction{
		BaseInstruction: l.newBase(ir.FlowInst),
		Name:            l.ctx.GetString(astWorkflow.NameRef),
	}

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

		if prevStep != nil {
			prevStep.Next = step.ID
		}

		if i == callCount-1 && assignment != "" {
			step.Assignment = assignment
		}

		l.registerInstruction(step)
		prevStep = step
	}

	return firstStepID, nil
}

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

		if stepRef, ok := l.mapStep[name]; ok {
			binding := l.ctx.StepBindingNodes[stepRef]
			fn := l.ctx.FunctionRefNodes[binding.RefRef]
			
			step.Call = []string{
				l.ctx.GetString(fn.ModuleRef),
				l.ctx.GetString(fn.NameRef),
			}

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

	if call.TrapRef.Start != call.TrapRef.End {
		handlerName := l.ctx.GetString(call.TrapRef)
		if id, ok := l.handlerIDs[handlerName]; ok {
			step.Handler = id
		}
	}

	return step, nil
}

func (l *Lowerer) registerInstruction(inst ir.Instruction) {
	l.program.Instructions[inst.GetID()] = inst
}

func (l *Lowerer) newBase(t ir.InstructionType) ir.BaseInstruction {
	return ir.BaseInstruction{
		ID:   uuid.New().String(),
		Type: t,
	}
}
