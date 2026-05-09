package compiler

import (
	"fmt"
	"strconv"
	"strings"

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

	handlerIDs map[string]string // Maps handler names to their generated IR IDs for trap resolution.
}

// Lower translates the provided AST ProgramNode into a ProgramIR.
// It executes four distinct passes to ensure all cross-references (traps, resources)
// are correctly resolved across the workflow topology.
func (l *Lowerer) Lower(astProgram ast.ProgramNode) (*ir.ProgramIR, error) {
	// Pass 1: Symbol Registration. Populates lookup tables for all top-level definitions.
	l.mapSymbol(astProgram)

	// Pass 2: Lowering Symbols. Generates IR for top-level definitions.
	if err := l.lowerSymbols(); err != nil {
		return nil, err
	}

	// Pass 3: Workflow Lowering. Translates main workflow blocks into execution entry points.
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

func (l *Lowerer) mapSymbol(astProgram ast.ProgramNode) {
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
}

func (l *Lowerer) lowerSymbols() error {
	for _, ref := range l.mapResource {
		node := l.ctx.ResourceNodes[ref]
		res, err := l.lowerResource(node)
		if err != nil {
			return err
		}
		l.registerInstruction(res)
	}

	for _, ref := range l.mapStep {
		node := l.ctx.StepBindingNodes[ref]
		res, err := l.lowerStep(node)
		if err != nil {
			return err
		}
		l.registerInstruction(res)
	}

	for name, ref := range l.mapHandler {
		node := l.ctx.HandlerNodes[ref]
		l.handlerIDs[name] = uuid.New().String()

		flow, err := l.lowerHandler(node)
		if err != nil {
			return err
		}
		l.registerInstruction(flow)
	}

	return nil
}

// lowerResource transforms an AST resource node into a ResourceInstruction.
func (l *Lowerer) lowerResource(astResource ast.ResourceBindingNode) (*ir.ResourceInstruction, error) {
	res := &ir.ResourceInstruction{
		BaseInstruction: l.newBase(ir.ResourceInst),
		Name:            l.ctx.GetString(astResource.NameRef),
		Config:          make(map[string]any),
	}

	// Resolve the resource provider (e.g. pg.connection).
	fnRef := l.ctx.FunctionRefNodes[astResource.FunctionRef]
	res.Provider = []string{
		l.ctx.GetString(fnRef.ModuleRef),
		l.ctx.GetString(fnRef.NameRef),
	}

	// Copy configuration from the resource definition.
	if fnRef.ConfigRef != 0 {
		dict := l.ctx.DictNodes[fnRef.ConfigRef]
		for j := dict.PairRefsStart; j < dict.PairRefsEnd; j++ {
			pairRef := l.ctx.PairRefs[j]
			pair := l.ctx.PairNodes[pairRef]
			key := l.ctx.GetString(pair.KeyRef)
			val, err := l.lowerLiteral(l.ctx.LiteralNodes[pair.ValueRef])
			if err != nil {
				return nil, err
			}
			res.Config[key] = val
		}
	}

	return res, nil
}

func (l *Lowerer) lowerStep(astStep ast.StepBindingNode) (*ir.StepInstruction, error) {
	return nil, nil
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
		handlerStatementRefs := l.ctx.HandlerStatementRefs[i]
		handlerStatement := l.ctx.HandlerStatementNodes[handlerStatementRefs]
		pipelineStatement := l.ctx.PipelineStatementNodes[handlerStatement.StmtRef]
		headID, _, err := l.lowerPipeline(pipelineStatement)
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

	vars := make(map[string]string) // Maps variable names to the ID of the step that produced them.

	for i := astWorkflow.StatementRefsStart; i < astWorkflow.StatementRefsEnd; i++ {
		psRef := l.ctx.StatementRefs[i]
		ps := l.ctx.PipelineStatementNodes[psRef]

		// Determine if this pipeline starts with a variable reference.
		pc := l.ctx.PipeChainNodes[ps.ExprRef]
		firstCallRef := l.ctx.CallRefs[pc.CallRefsStart]
		firstCall := l.ctx.CallNodes[firstCallRef]

		if !firstCall.IsPrql && firstCall.NameRef.Start != firstCall.NameRef.End {
			name := l.ctx.GetString(firstCall.NameRef)
			if producerID, ok := vars[name]; ok {
				// This pipeline consumes a variable.
				callCount := pc.CallRefsEnd - pc.CallRefsStart
				if callCount > 1 {
					// Create a sub-chain from the remaining steps.
					subPC := pc
					subPC.CallRefsStart++

					// Optimization: If the next call is identical to the producer, skip it.
					// This handles the redundant `output | io.print` pattern when `print` is aliased to `io.print`.
					nextCallRef := l.ctx.CallRefs[subPC.CallRefsStart]
					nextCall := l.ctx.CallNodes[nextCallRef]
					producer := l.program.Instructions[producerID].(*ir.StepInstruction)

					if !nextCall.IsPrql && l.ctx.GetString(nextCall.NameRef) == producer.DefinitionName {
						// Skip this call and move to the next.
						subPC.CallRefsStart++
						if subPC.CallRefsStart == subPC.CallRefsEnd {
							// Nothing left.
							if ps.AssignmentRef.Start != ps.AssignmentRef.End {
								vars[l.ctx.GetString(ps.AssignmentRef)] = producerID
							}
							continue
						}
					}

					headID, tailID, err := l.lowerPipeChain(subPC, l.ctx.GetString(ps.AssignmentRef))
					if err != nil {
						return nil, err
					}

					// Link the producer step to the new chain's head.
					if producer, exists := l.program.Instructions[producerID].(*ir.StepInstruction); exists {
						producer.Next = headID
					}

					if ps.AssignmentRef.Start != ps.AssignmentRef.End {
						vars[l.ctx.GetString(ps.AssignmentRef)] = tailID
					}
					continue
				} else {
					// Edge case: pipeline is just a variable reference with no following steps.
					if ps.AssignmentRef.Start != ps.AssignmentRef.End {
						vars[l.ctx.GetString(ps.AssignmentRef)] = producerID
					}
					continue
				}
			}
		}

		// Standard pipeline or one that doesn't start with a known variable.
		headID, tailID, err := l.lowerPipeline(ps)
		if err != nil {
			return nil, err
		}
		flow.Heads = append(flow.Heads, headID)

		if ps.AssignmentRef.Start != ps.AssignmentRef.End {
			vars[l.ctx.GetString(ps.AssignmentRef)] = tailID
		}
	}

	return flow, nil
}

// lowerPipeline dispatches a pipeline statement to its specific lowering logic.
func (l *Lowerer) lowerPipeline(pipelineStatement ast.PipelineStatementNode) (string, string, error) {
	ref := pipelineStatement.ExprRef
	if ref == 0 {
		return "", "", nil
	}

	if int(ref) < len(l.ctx.PipeChainNodes) {
		pipeChain := l.ctx.PipeChainNodes[ref]
		return l.lowerPipeChain(pipeChain, l.ctx.GetString(pipelineStatement.AssignmentRef))
	}

	return "", "", nil
}

// lowerPipeChain connects a sequence of calls into a linked list of IR instructions.
func (l *Lowerer) lowerPipeChain(pc ast.PipeChainNode, assignment string) (string, string, error) {
	var prevStep *ir.StepInstruction
	var firstStepID string
	var lastStepID string

	callCount := pc.CallRefsEnd - pc.CallRefsStart

	for i := range uint32(callCount) {
		callRef := l.ctx.CallRefs[pc.CallRefsStart+i]
		call := l.ctx.CallNodes[callRef]
		step, err := l.lowerCall(call)
		if err != nil {
			return "", "", err
		}

		if i == 0 {
			firstStepID = step.ID
		}
		if i == callCount-1 {
			lastStepID = step.ID
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

	return firstStepID, lastStepID, nil
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
			fn := l.ctx.FunctionRefNodes[binding.FunctionRef]

			step.Call = []string{
				l.ctx.GetString(fn.ModuleRef),
				l.ctx.GetString(fn.NameRef),
			}

			// Copy configuration from the binding definition.
			if fn.ConfigRef != 0 {
				dict := l.ctx.DictNodes[fn.ConfigRef]
				for j := dict.PairRefsStart; j < dict.PairRefsEnd; j++ {
					pairRef := l.ctx.PairRefs[j]
					pair := l.ctx.PairNodes[pairRef]
					key := l.ctx.GetString(pair.KeyRef)
					val, err := l.lowerLiteral(l.ctx.LiteralNodes[pair.ValueRef])
					if err != nil {
						return nil, err
					}
					step.Config[key] = val
				}
			}

			// Map resource requirements to the step configuration.
			if fn.ResourcesRefRef != 0 {
				rr := l.ctx.ResourceRefNodes[fn.ResourcesRefRef]
				for i := rr.MappingsRefStart; i < rr.MappingsRefEnd; i++ {
					mappingRef := l.ctx.MappingRefs[i]
					mapping := l.ctx.ResourceMappingNodes[mappingRef]
					step.Resources[l.ctx.GetString(mapping.KeyRef)] = l.ctx.GetString(mapping.ValueRef)
				}
			}
		} else {
			// Resolve via module-qualified name used directly.
			if before, after, ok0 := strings.Cut(name, "."); ok0 {
				module := before
				fn := after
				step.Call = []string{module, fn}
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

// lowerLiteral recursively transforms an AST literal node into a native Go value.
func (l *Lowerer) lowerLiteral(lit ast.LiteralNode) (any, error) {
	switch lit.Type {
	case ast.LiteralString:
		return l.ctx.GetString(lit.ValueRef), nil

	case ast.LiteralInt:
		val, err := strconv.ParseInt(l.ctx.GetString(lit.ValueRef), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse integer literal: %w", err)
		}
		return val, nil

	case ast.LiteralFloat:
		val, err := strconv.ParseFloat(l.ctx.GetString(lit.ValueRef), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float literal: %w", err)
		}
		return val, nil

	case ast.LiteralBool:
		return l.ctx.GetString(lit.ValueRef) == "true", nil

	case ast.LiteralNull:
		return nil, nil

	case ast.LiteralDict:
		dict := l.ctx.DictNodes[lit.Ref]
		res := make(map[string]any)
		for i := dict.PairRefsStart; i < dict.PairRefsEnd; i++ {
			pairRef := l.ctx.PairRefs[i]
			pair := l.ctx.PairNodes[pairRef]
			key := l.ctx.GetString(pair.KeyRef)
			val, err := l.lowerLiteral(l.ctx.LiteralNodes[pair.ValueRef])
			if err != nil {
				return nil, err
			}
			res[key] = val
		}
		return res, nil

	case ast.LiteralList:
		list := l.ctx.ListNodes[lit.Ref]
		res := make([]any, 0)
		for i := list.LiteralRefsStart; i < list.LiteralRefsEnd; i++ {
			litRef := l.ctx.LiteralRefs[i]
			val, err := l.lowerLiteral(l.ctx.LiteralNodes[litRef])
			if err != nil {
				return nil, err
			}
			res = append(res, val)
		}
		return res, nil
	}

	return nil, nil
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
