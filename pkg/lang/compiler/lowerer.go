package compiler

import (
	"fmt"
	"strconv"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"go.uber.org/zap"
)

// Lowerer is the compiler component responsible for transforming the high-level AST
// into a flat, serializable Intermediate Representation (IR). It performs symbol
// resolution, resource injection, and DAG construction.
type Lowerer struct {
	program *ast.ProgramNode
	ctx     *ast.ASTContext

	// instructions stores all generated IR instructions indexed by their unique IDs.
	instructions map[string]any

	// workflows preserves the order of entry-point workflow IDs.
	workflows []string

	// Maps used for symbol resolution across compilation phases.
	importMap   map[string]string // alias -> canonical module path
	aliasMap    map[string]string // workflow-local assignment -> producer instruction ID
	resourceMap map[string]string // alias -> resource instruction ID
	stepMap     map[string]string // alias -> base step instruction ID
	handlerMap  map[string]string // name  -> handler head instruction ID
}

// Lower executes the multi-pass transformation from AST to Program.
// The process follows a strict resolution order: Imports -> Resources -> Steps -> Handlers -> Workflows.
func (l *Lowerer) Lower(program ast.ProgramNode) (*ir.Program, error) {
	l.program = &program

	// Phase 1: Establish module namespaces.
	if err := l.lowerImports(); err != nil {
		return nil, err
	}

	// Phase 2: Register centralized state and connection resources.
	if err := l.lowerResources(); err != nil {
		return nil, err
	}

	// Phase 3: Prepare bound imperative step definitions for reuse.
	if err := l.lowerSteps(); err != nil {
		return nil, err
	}

	// Phase 4: Construct reusable error handling pipelines.
	if err := l.lowerHandlers(); err != nil {
		return nil, err
	}

	// Phase 5: Assemble the execution DAG for each workflow entry point.
	if err := l.lowerWorkflows(); err != nil {
		return nil, err
	}

	return &ir.Program{
		BaseInstruction: ir.BaseInstruction{
			ID:   "program",
			Type: ir.ProgramInst,
		},
		Instructions: l.instructions,
		Workflows:    l.workflows,
	}, nil
}

// lowerImports processes module import declarations and populates the import namespace map.
func (l *Lowerer) lowerImports() error {
	for i := l.program.ImportRefsStart; i < l.program.ImportRefsEnd; i++ {
		nodeRef := l.ctx.ImportRefs[i]
		node := l.ctx.ImportNodes[nodeRef]
		path := l.getString(node.PathRef)
		alias := l.getString(node.AliasRef)

		inst := &ir.ImportInstruction{
			BaseInstruction: ir.BaseInstruction{
				ID:   l.nextID("import"),
				Type: ir.ImportInst,
			},
			Path:  path,
			Alias: alias,
		}

		// Register the alias for subsequent module-qualified reference resolution.
		l.importMap[alias] = path
		l.addInstruction(inst)
	}
	return nil
}

// lowerResources transforms resource bindings into IR instructions, resolving provider paths.
func (l *Lowerer) lowerResources() error {
	for i := l.program.ResourceRefsStart; i < l.program.ResourceRefsEnd; i++ {
		nodeRef := l.ctx.ResourceRefs[i]
		node := l.ctx.ResourceNodes[nodeRef]
		name := l.getString(node.NameRef)
		fnRef := l.ctx.FunctionRefNodes[node.FunctionRef]

		config, err := l.lowerDict(fnRef.ConfigRef)
		if err != nil {
			return err
		}

		// Resolve the provider module through the established import map.
		module := l.getString(fnRef.ModuleRef)
		if path, ok := l.importMap[module]; ok {
			module = path
		}

		inst := &ir.ResourceInstruction{
			BaseInstruction: ir.BaseInstruction{
				ID:   l.nextID("resource"),
				Type: ir.ResourceInst,
			},
			Name:     name,
			Provider: []string{module, l.getString(fnRef.NameRef)},
			Config:   config,
		}

		l.resourceMap[name] = inst.ID
		l.addInstruction(inst)
	}
	return nil
}

// lowerSteps transforms named step definitions into base instructions for later instantiation.
func (l *Lowerer) lowerSteps() error {
	for i := l.program.StepRefsStart; i < l.program.StepRefsEnd; i++ {
		nodeRef := l.ctx.StepRefs[i]
		node := l.ctx.StepBindingNodes[nodeRef]
		name := l.getString(node.NameRef)
		fnRef := l.ctx.FunctionRefNodes[node.FunctionRef]

		config, err := l.lowerDict(fnRef.ConfigRef)
		if err != nil {
			return err
		}

		module := l.getString(fnRef.ModuleRef)
		if path, ok := l.importMap[module]; ok {
			module = path
		}

		inst := &ir.StepInstruction{
			BaseInstruction: ir.BaseInstruction{
				ID:   l.nextID("step"),
				Type: ir.StepInst,
			},
			DefinitionName: name,
			Call:           []string{module, l.getString(fnRef.NameRef)},
			Resources:      l.lowerResourceRefs(fnRef.ResourcesRefRef),
			Config:         config,
		}

		l.stepMap[name] = inst.ID
		l.addInstruction(inst)
	}
	return nil
}

// lowerResourceRefs extracts and maps resource key-value pairs from the AST.
func (l *Lowerer) lowerResourceRefs(ref ast.NodeRef) map[string]string {
	if ref == ast.NilNode {
		return make(map[string]string)
	}

	resRef := l.ctx.ResourceRefNodes[ref]
	result := make(map[string]string)

	for i := resRef.MappingsRefStart; i < resRef.MappingsRefEnd; i++ {
		mappingRef := l.ctx.MappingRefs[i]
		mapping := l.ctx.ResourceMappingNodes[mappingRef]
		key := l.getString(mapping.KeyRef)
		val := l.getString(mapping.ValueRef)
		result[key] = val
	}

	return result
}

// lowerHandlers transforms handler declarations into isolated sub-pipelines.
func (l *Lowerer) lowerHandlers() error {
	for i := l.program.HandlerRefsStart; i < l.program.HandlerRefsEnd; i++ {
		nodeRef := l.ctx.HandlerRefs[i]
		node := l.ctx.HandlerNodes[nodeRef]
		name := l.getString(node.NameRef)

		// A handler head represents the first instruction in its execution chain.
		headID, err := l.lowerHandlerStatements(node)
		if err != nil {
			return err
		}

		l.handlerMap[name] = headID
	}
	return nil
}

// lowerHandlerStatements chains multiple statements within a handler into a single pipeline.
func (l *Lowerer) lowerHandlerStatements(node ast.HandlerNode) (string, error) {
	var headID, prevTailID string
	for i := node.HandlerStatementRefsStart; i < node.HandlerStatementRefsEnd; i++ {
		stmtRef := l.ctx.HandlerStatementRefs[i]
		stmtNode := l.ctx.HandlerStatementNodes[stmtRef]

		hStmt, tailID, err := l.lowerPipelineStatement(l.ctx.PipelineStatementNodes[stmtNode.StmtRef], stmtNode.IsCatchAll)
		if err != nil {
			return "", err
		}

		if headID == "" {
			headID = hStmt
		}
		if prevTailID != "" {
			// Join the previous statement's tail to the current statement's head.
			if prevStep, ok := l.instructions[prevTailID].(*ir.StepInstruction); ok {
				prevStep.Next = append(prevStep.Next, hStmt)
				if nextStep, ok := l.instructions[hStmt].(*ir.StepInstruction); ok {
					nextStep.Parents = append(nextStep.Parents, prevTailID)
				}
			}
		}
		prevTailID = tailID
	}
	return headID, nil
}

// lowerWorkflows constructs the main execution DAGs, linking pipelines and traps.
func (l *Lowerer) lowerWorkflows() error {
	for i := l.program.WorkflowRefsStart; i < l.program.WorkflowRefsEnd; i++ {
		nodeRef := l.ctx.WorkflowRefs[i]
		node := l.ctx.WorkflowNodes[nodeRef]
		name := l.getString(node.NameRef)

		flow := &ir.FlowInstruction{
			BaseInstruction: ir.BaseInstruction{
				ID:   l.nextID("workflow"),
				Type: ir.FlowInst,
			},
			Name:  name,
			Heads: make([]string, 0),
		}

		// Resolve and bind the global error trap if specified.
		if node.TrapRef.End > 0 {
			trapName := l.getString(node.TrapRef)
			flow.Handler = l.handlerMap[trapName]
		}

		// Assemble statements and establish data-driven execution order.
		for j := node.StatementRefsStart; j < node.StatementRefsEnd; j++ {
			stmtRef := l.ctx.StatementRefs[j]
			stmt := l.ctx.PipelineStatementNodes[stmtRef]

			headID, _, err := l.lowerPipelineStatement(stmt, false)
			if err != nil {
				return err
			}

			// A step is a workflow head if it has no incoming data dependencies (parents).
			if headID != "" {
				if step, ok := l.instructions[headID].(*ir.StepInstruction); ok {
					if len(step.Parents) == 0 {
						flow.Heads = append(flow.Heads, headID)
					}
				}
			}
		}

		l.workflows = append(l.workflows, flow.ID)
		l.addInstruction(flow)
	}
	return nil
}

// lowerPipelineStatement converts a pipeline of calls into a linked chain of IR instructions.
func (l *Lowerer) lowerPipelineStatement(stmt ast.PipelineStatementNode, isCatchAll bool) (headID, tailID string, err error) {
	// A statement represents either a PipeChain or a specialized Dataframe operation.
	chain := l.ctx.PipeChainNodes[stmt.ExprRef]

	var lastStepID string
	for i := chain.CallRefsStart; i < chain.CallRefsEnd; i++ {
		callRef := l.ctx.CallRefs[i]
		call := l.ctx.CallNodes[callRef]

		// Structural placeholders (empty calls) are only skipped if they are not the
		// initial node of a pipeline. In handlers, an empty call (represented by '*')
		// is preserved as an identity step to facilitate DAG visualization.
		if i > chain.CallRefsStart &&
			call.NameRef.End == 0 &&
			call.FunctionRef == ast.NilNode &&
			!call.IsPrql &&
			call.DataframeRef == ast.NilNode {
			logger.L().Warn("Skipping structural placeholder call", zap.Any("call", call))
			continue
		}

		stepID := ""
		isEmpty := call.NameRef.End == 0 &&
			call.FunctionRef == ast.NilNode &&
			!call.IsPrql &&
			call.DataframeRef == ast.NilNode

		if isEmpty {
			// Identity step representing '*' or implicit input redirection.
			stepID = l.nextID("step_identity")
			inst := &ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{
					ID:   stepID,
					Type: ir.StepInst,
				},
				DefinitionName: "identity",
				Call:           []string{"__internal", "identity"},
				Config:         make(map[string]any),
			}
			if isCatchAll {
				inst.Config["is_catch_all"] = true
			}
			l.addInstruction(inst)

		} else if call.IsPrql {
			// Handle inline relational transformations via the standard query step.
			stepID = l.nextID("step_prql")
			inst := &ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{
					ID:   stepID,
					Type: ir.StepInst,
				},
				DefinitionName: "prql",
				Call:           []string{"__internal", "prql"},
				Config:         map[string]any{"query": l.getString(call.QueryRef)},
			}
			if call.TrapRef.End > 0 {
				trapName := l.getString(call.TrapRef)
				inst.Handler = l.handlerMap[trapName]
			}
			l.addInstruction(inst)

		} else if call.NameRef.End > 0 {
			name := l.getString(call.NameRef)

			// Logic Pattern: Variable Resolution -> Step Instantiation.
			// 1. Resolve workflow-local aliases from previous assignments.
			if aliasID, ok := l.aliasMap[name]; ok {
				if headID == "" {
					lastStepID = aliasID
					continue
				}
				continue
			}

			// 2. Resolve bound step definitions and create a unique execution instance.
			origID, ok := l.stepMap[name]
			if !ok {
				return "", "", fmt.Errorf("undefined step: %s", name)
			}

			stepID = l.nextID("step_call")
			orig := l.instructions[origID].(*ir.StepInstruction)

			// Clone the base definition to ensure call-specific state isolation.
			inst := *orig
			inst.ID = stepID
			if call.TrapRef.End > 0 {
				trapName := l.getString(call.TrapRef)
				inst.Handler = l.handlerMap[trapName]
			}
			l.addInstruction(&inst)

		} else if call.DataframeRef != ast.NilNode {
			// Handle dataframe literals in pipelines by creating a specialized data step.
			stepID = l.nextID("step_data")
			data, err := l.lowerDataframe(call.DataframeRef)
			if err != nil {
				return "", "", err
			}

			inst := &ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{
					ID:   stepID,
					Type: ir.StepInst,
				},
				DefinitionName: "data_literal",
				Call:           []string{"__internal", "data_literal"},
				Config:         map[string]any{"data": data},
			}
			if call.TrapRef.End > 0 {
				trapName := l.getString(call.TrapRef)
				inst.Handler = l.handlerMap[trapName]
			}
			l.addInstruction(inst)

		} else {
			// Handle anonymous calls by resolving modules and generating fresh instructions.
			fnRef := l.ctx.FunctionRefNodes[call.FunctionRef]
			stepID = l.nextID("step_call_anonymous")

			config, err := l.lowerDict(fnRef.ConfigRef)
			if err != nil {
				return "", "", err
			}

			module := l.getString(fnRef.ModuleRef)
			if path, ok := l.importMap[module]; ok {
				module = path
			}

			inst := &ir.StepInstruction{
				BaseInstruction: ir.BaseInstruction{
					ID:   stepID,
					Type: ir.StepInst,
				},
				Call:      []string{module, l.getString(fnRef.NameRef)},
				Resources: l.lowerResourceRefs(fnRef.ResourcesRefRef),
				Config:    config,
			}
			if call.TrapRef.End > 0 {
				trapName := l.getString(call.TrapRef)
				inst.Handler = l.handlerMap[trapName]
			}
			l.addInstruction(inst)
		}

		if headID == "" {
			headID = stepID
		}
		if lastStepID != "" {
			// Link current step as the successor to the previous one.
			if prevStep, ok := l.instructions[lastStepID].(*ir.StepInstruction); ok {
				prevStep.Next = append(prevStep.Next, stepID)
				if nextStep, ok := l.instructions[stepID].(*ir.StepInstruction); ok {
					nextStep.Parents = append(nextStep.Parents, lastStepID)
				}
			}
		}
		lastStepID = stepID
	}

	// Record assignment targets to enable later variable reference resolution.
	if stmt.AssignmentRef.End > 0 {
		aliasName := l.getString(stmt.AssignmentRef)
		if lastStep, ok := l.instructions[lastStepID].(*ir.StepInstruction); ok {
			lastStep.Assignment = aliasName
			l.aliasMap[aliasName] = lastStepID
		}
	}

	return headID, lastStepID, nil
}

// lowerLiteral recursively transforms AST literal nodes into corresponding Go types for IR configuration.
func (l *Lowerer) lowerLiteral(ref ast.NodeRef) (any, error) {
	if ref == ast.NilNode {
		return nil, nil
	}

	node := l.ctx.LiteralNodes[ref]
	switch node.Type {
	case ast.LiteralString:
		return l.getString(node.ValueRef), nil
	case ast.LiteralInt:
		val, err := strconv.ParseInt(l.getString(node.ValueRef), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse int: %w", err)
		}
		return val, nil
	case ast.LiteralFloat:
		val, err := strconv.ParseFloat(l.getString(node.ValueRef), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float: %w", err)
		}
		return val, nil
	case ast.LiteralBool:
		return l.getString(node.ValueRef) == "true", nil
	case ast.LiteralNull:
		return nil, nil
	case ast.LiteralDict:
		return l.lowerDict(node.Ref)
	case ast.LiteralList:
		listNode := l.ctx.ListNodes[node.Ref]
		result := make([]any, 0)
		for i := listNode.LiteralRefsStart; i < listNode.LiteralRefsEnd; i++ {
			val, err := l.lowerLiteral(l.ctx.LiteralRefs[i])
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil
	default:
		return nil, nil
	}
}

func (l *Lowerer) getString(ref ast.StringRef) string {
	return l.ctx.GetString(ref)
}

// lowerDict transforms AST dictionary nodes into Go maps.
func (l *Lowerer) lowerDict(ref ast.NodeRef) (map[string]any, error) {
	if ref == ast.NilNode {
		return make(map[string]any), nil
	}

	dictNode := l.ctx.DictNodes[ref]
	result := make(map[string]any)

	for i := dictNode.PairRefsStart; i < dictNode.PairRefsEnd; i++ {
		pairRef := l.ctx.PairRefs[i]
		pair := l.ctx.PairNodes[pairRef]
		key := l.getString(pair.KeyRef)
		val, err := l.lowerLiteral(pair.ValueRef)
		if err != nil {
			return nil, err
		}
		result[key] = val
	}

	return result, nil
}

// lowerDataframe transforms an AST dataframe node into a Go slice of maps for IR configuration.
func (l *Lowerer) lowerDataframe(ref ast.NodeRef) ([]map[string]any, error) {
	if ref == ast.NilNode {
		return nil, nil
	}

	node := l.ctx.DataframeNodes[ref]
	result := make([]map[string]any, 0)

	for i := node.DictRefsStart; i < node.DictRefsEnd; i++ {
		dictRef := l.ctx.DictRefs[i]
		dict, err := l.lowerDict(dictRef)
		if err != nil {
			return nil, err
		}
		result = append(result, dict)
	}

	return result, nil
}

// addInstruction registers a new instruction in the program's global instruction map.
func (l *Lowerer) addInstruction(inst ir.Instruction) {
	l.instructions[inst.GetID()] = inst
}

// nextID generates a unique identifier for an IR instruction based on the given prefix.
func (l *Lowerer) nextID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, len(l.instructions))
}

// NewLowerer creates a new Lowerer instance.
func NewLowerer(ctx *ast.ASTContext) *Lowerer {
	return &Lowerer{
		ctx:          ctx,
		instructions: make(map[string]any),
		workflows:    make([]string, 0),
		importMap:    make(map[string]string),
		aliasMap:     make(map[string]string),
		resourceMap:  make(map[string]string),
		stepMap:      make(map[string]string),
		handlerMap:   make(map[string]string),
	}
}
