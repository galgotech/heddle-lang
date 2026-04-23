package compiler

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/ir"
)

// Lowerer handles the translation of the Abstract Syntax Tree (AST) into
// the Intermediate Representation (IR).
type Lowerer struct {
	program *ir.ProgramIR

	// Maps for resolving symbols during lowering
	mapImport   map[string]string // alias -> module path
	mapSchema   map[string]*ast.SchemaDefinition
	mapResource map[string]*ast.ResourceBinding
	mapStep     map[string]*ast.StepBinding
	mapHandler  map[string]*ast.HandlerDefinition
}

// NewLowerer creates a new instance of the Lowerer.
func NewLowerer() *Lowerer {
	return &Lowerer{
		program: &ir.ProgramIR{
			BaseInstruction: ir.BaseInstruction{
				ID:   uuid.New().String(),
				Type: ir.ProgramInst,
			},
			Instructions: make(map[string]interface{}),
			Workflows:    []string{},
		},
		mapImport:   make(map[string]string),
		mapSchema:   make(map[string]*ast.SchemaDefinition),
		mapResource: make(map[string]*ast.ResourceBinding),
		mapStep:     make(map[string]*ast.StepBinding),
		mapHandler:  make(map[string]*ast.HandlerDefinition),
	}
}

// Lower translates an AST Program into a ProgramIR.
func (l *Lowerer) Lower(astProgram *ast.Program) (*ir.ProgramIR, error) {
	// First pass: Register all definitions for resolution
	for _, stmt := range astProgram.Statements {
		switch s := stmt.(type) {
		case *ast.ImportStatement:
			l.mapImport[s.Alias.Value] = s.Path.Value
		case *ast.SchemaDefinition:
			l.mapSchema[s.Name.Value] = s
		case *ast.ResourceBinding:
			l.mapResource[s.Name.Value] = s
		case *ast.StepBinding:
			l.mapStep[s.Name.Value] = s
		case *ast.HandlerDefinition:
			l.mapHandler[s.Name.Value] = s
		}
	}

	// Second pass: Lower resources
	for _, rb := range l.mapResource {
		res, err := l.lowerResource(rb)
		if err != nil {
			return nil, err
		}
		l.registerInstruction(res)
	}

	// Third pass: Lower workflows
	for _, stmt := range astProgram.Statements {
		if wd, ok := stmt.(*ast.WorkflowDefinition); ok {
			flow, err := l.lowerWorkflow(wd)
			if err != nil {
				return nil, err
			}
			l.registerInstruction(flow)
			l.program.Workflows = append(l.program.Workflows, flow.ID)
		}
	}

	return l.program, nil
}

func (l *Lowerer) lowerResource(astResource *ast.ResourceBinding) (*ir.ResourceInstruction, error) {
	res := &ir.ResourceInstruction{
		BaseInstruction: l.newBase(ir.ResourceInst, astResource),
		Name:            astResource.Name.Value,
		Provider:        l.resolveCall(astResource.Ref),
		Config:          l.lowerDictionary(astResource.Ref.Config),
	}
	return res, nil
}

func (l *Lowerer) lowerWorkflow(astWorkflow *ast.WorkflowDefinition) (*ir.FlowInstruction, error) {
	flow := &ir.FlowInstruction{
		BaseInstruction: l.newBase(ir.FlowInst, astWorkflow),
		Name:            astWorkflow.Name.Value,
	}

	if astWorkflow.TrapHandler != nil {
		handlerID, err := l.lowerHandlerRef(astWorkflow.TrapHandler.Value)
		if err != nil {
			return nil, err
		}
		flow.Handler = handlerID
	}

	for _, ps := range astWorkflow.Statements {
		headID, err := l.lowerPipeline(ps)
		if err != nil {
			return nil, err
		}
		flow.Heads = append(flow.Heads, headID)
	}

	return flow, nil
}

func (l *Lowerer) lowerPipeline(ps *ast.PipelineStatement) (string, error) {
	switch expr := ps.Expression.(type) {
	case *ast.PipeChain:
		return l.lowerPipeChain(expr, ps.Assignment)
	case *ast.Dataframe:
		// TODO: Implement dataframe lowering (likely an ImmutableInstruction)
		return "", fmt.Errorf("dataframe lowering not implemented")
	}
	return "", fmt.Errorf("unknown pipeline expression type: %T", ps.Expression)
}

func (l *Lowerer) lowerPipeChain(pc *ast.PipeChain, assignment *ast.Identifier) (string, error) {
	var prevStep *ir.StepInstruction
	var firstStepID string

	for i, call := range pc.Calls {
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

		// If this is the last step in the chain and there's an assignment
		if i == len(pc.Calls)-1 && assignment != nil {
			step.Assignment = assignment.Value
		}

		l.registerInstruction(step)
		prevStep = step
	}

	return firstStepID, nil
}

func (l *Lowerer) lowerCall(call *ast.CallExpression) (*ir.StepInstruction, error) {
	step := &ir.StepInstruction{
		BaseInstruction: l.newBase(ir.StepInst, call),
	}

	if call.TrapHandler != nil {
		handlerID, err := l.lowerHandlerRef(call.TrapHandler.Name.Value)
		if err != nil {
			return nil, err
		}
		step.Handler = handlerID
		step.HandlerRedirectData = true // Default for step handlers
	}

	switch s := call.Step.(type) {
	case *ast.StepCall:
		if stepDef, ok := l.mapStep[s.Name.Value]; ok {
			step.DefinitionName = stepDef.Name.Value
			step.Call = l.resolveCall(stepDef.Ref)
			step.Config = l.lowerDictionary(stepDef.Ref.Config)
			step.Resources = stepDef.Ref.Resource
			step.InputType = l.lowerSchemaRefList(stepDef.Signature.Input)
			step.OutputType = l.lowerSchemaRefList(stepDef.Signature.Output)
		} else {
			return nil, fmt.Errorf("undefined step: %s", s.Name.Value)
		}
	case *ast.AnonymousStepExpression:
		step.DefinitionName = "anonymous"
		step.InputType = l.lowerSchemaRefList(s.Signature.Input)
		step.OutputType = l.lowerSchemaRefList(s.Signature.Output)
		if fr, ok := s.Ref.(*ast.FunctionRef); ok {
			step.Call = l.resolveCall(fr)
			step.Config = l.lowerDictionary(fr.Config)
			step.Resources = fr.Resource
		} else if prql, ok := s.Ref.(*ast.PRQLExpression); ok {
			step.Config = map[string]any{"query": prql.Value}
			step.Call = []string{"std", "relational", "prql"}
		}
	}

	return step, nil
}

func (l *Lowerer) lowerHandlerRef(name string) (string, error) {
	// Simple implementation: find the handler and lower its first step.
	// In a real compiler, we'd cache this.
	handler, ok := l.mapHandler[name]
	if !ok {
		return "", fmt.Errorf("undefined handler: %s", name)
	}

	// For now, we just lower the first pipeline in the handler as the head of the handler chain
	if len(handler.Statements) == 0 {
		return "", nil
	}

	// This is a simplification. Real handlers might have multiple statements.
	// But let's assume the first pipeline is the head.
	for _, stmt := range handler.Statements {
		if ps, ok := stmt.(*ast.PipelineStatement); ok {
			return l.lowerPipeline(ps)
		}
	}

	return "", nil
}

func (l *Lowerer) lowerDictionary(dict *ast.Dictionary) map[string]any {
	if dict == nil {
		return make(map[string]any)
	}
	res := make(map[string]any)
	for k, v := range dict.Pairs {
		res[k] = l.lowerExpression(v)
	}
	return res
}

func (l *Lowerer) lowerExpression(expr ast.Expression) any {
	switch e := expr.(type) {
	case *ast.StringLiteral:
		return strings.Trim(e.Value, "\"")
	case *ast.NumberLiteral:
		return e.Value
	case *ast.BooleanLiteral:
		return e.Value
	case *ast.NullLiteral:
		return nil
	case *ast.Dictionary:
		return l.lowerDictionary(e)
	case *ast.List:
		list := []any{}
		for _, item := range e.Elements {
			list = append(list, l.lowerExpression(item))
		}
		return list
	}
	return nil
}

func (l *Lowerer) lowerSchemaRefList(node ast.Node) []string {
	if node == nil {
		return nil
	}
	if _, ok := node.(*ast.VoidType); ok {
		return nil
	}
	if sr, ok := node.(*ast.SchemaRef); ok {
		if sr.Module != nil {
			return []string{sr.Module.Value, sr.Name.Value}
		}
		return []string{sr.Name.Value}
	}
	return nil
}

func (l *Lowerer) resolveCall(fr *ast.FunctionRef) []string {
	module := fr.Module.Value
	if realModule, ok := l.mapImport[module]; ok {
		module = strings.Trim(realModule, "\"")
	}
	return []string{module, fr.Name.Value}
}

func (l *Lowerer) registerInstruction(inst ir.Instruction) {
	l.program.Instructions[inst.GetID()] = inst
}

func (l *Lowerer) newBase(t ir.InstructionType, node ast.Node) ir.BaseInstruction {
	// In a real implementation, we'd extract line/col from the node/token
	return ir.BaseInstruction{
		ID:   uuid.New().String(),
		Type: t,
	}
}
