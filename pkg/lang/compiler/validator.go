package compiler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

type DiagnosticSeverity int

const (
	SeverityError DiagnosticSeverity = iota
	SeverityWarning
	SeverityInformation
	SeverityHint
)

type DiagnosticTag int

const (
	TagUnnecessary DiagnosticTag = iota + 1
	TagDeprecated
)

// ValidationError represents a diagnostic entry encountered during validation.
type ValidationError struct {
	Message  string
	Range    ast.Range
	Severity DiagnosticSeverity
	Tags     []DiagnosticTag
}

// Validator performs semantic analysis on the AST.
type Validator struct {
	program ast.ProgramNode
	ctx     *ast.ASTContext
	schemas map[string]schema.StepSchemas

	// Symbol tables mapping name to NodeRef
	mapResource map[string]ast.NodeRef
	mapStep     map[string]ast.NodeRef
	mapHandler  map[string]ast.NodeRef
	mapWorkflow map[string]ast.NodeRef
	mapImport   map[string]ast.NodeRef

	// Usage tracking
	usedResources map[string]bool
	usedSteps     map[string]bool
	usedHandlers  map[string]bool
	usedImports   map[string]bool
	assignments   map[string]ast.NodeRef // map name to assignment node ref
	usedVariables map[string]bool

	errors []ValidationError
}

// NewValidator creates a new instance of the Validator.
func NewValidator(program ast.ProgramNode, ctx *ast.ASTContext, schemas map[string]schema.StepSchemas) *Validator {
	return &Validator{
		program:       program,
		ctx:           ctx,
		schemas:       schemas,
		mapResource:   make(map[string]ast.NodeRef),
		mapStep:       make(map[string]ast.NodeRef),
		mapHandler:    make(map[string]ast.NodeRef),
		mapWorkflow:   make(map[string]ast.NodeRef),
		mapImport:     make(map[string]ast.NodeRef),
		usedResources: make(map[string]bool),
		usedSteps:     make(map[string]bool),
		usedHandlers:  make(map[string]bool),
		usedImports:   make(map[string]bool),
		assignments:   make(map[string]ast.NodeRef),
		usedVariables: make(map[string]bool),
	}
}

// ValidateAll runs all semantic checks and returns all encountered errors.
func (v *Validator) ValidateAll() []ValidationError {
	v.errors = nil
	v.registerDefinitions()
	v.validateReferencesAll()
	v.checkUnused()
	v.detectCyclesAll()
	return v.errors
}

func (v *Validator) addError(msg string, line, col uint32) {
	v.errors = append(v.errors, ValidationError{
		Message: msg,
		Range: ast.Range{
			Start: ast.Position{Line: line, Col: col},
			End:   ast.Position{Line: line, Col: col + 1},
		},
		Severity: SeverityError,
	})
}

func (v *Validator) addWarning(msg string, line, col uint32, tags ...DiagnosticTag) {
	v.errors = append(v.errors, ValidationError{
		Message: msg,
		Range: ast.Range{
			Start: ast.Position{Line: line, Col: col},
			End:   ast.Position{Line: line, Col: col + 1},
		},
		Severity: SeverityWarning,
		Tags:     tags,
	})
}

func (v *Validator) addErrorAtRange(msg string, r ast.Range) {
	v.errors = append(v.errors, ValidationError{
		Message:  msg,
		Range:    r,
		Severity: SeverityError,
	})
}

func (v *Validator) addWarningAtRange(msg string, r ast.Range, tags ...DiagnosticTag) {
	v.errors = append(v.errors, ValidationError{
		Message:  msg,
		Range:    r,
		Severity: SeverityWarning,
		Tags:     tags,
	})
}

// Validate runs all semantic checks and returns the first error encountered.
func (v *Validator) Validate() error {
	errs := v.ValidateAll()
	if len(errs) > 0 {
		return fmt.Errorf("%s (line %d, col %d)", errs[0].Message, errs[0].Range.Start.Line, errs[0].Range.Start.Col)
	}
	return nil
}

func (v *Validator) getImportNamespace(node ast.ImportNode) string {
	if node.AliasRef.Start != node.AliasRef.End {
		return v.ctx.GetString(node.AliasRef)
	}
	path := v.ctx.GetString(node.PathRef)
	if len(path) >= 2 && path[0] == '"' && path[len(path)-1] == '"' {
		path = path[1 : len(path)-1]
	}
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return path
}

func (v *Validator) checkCollision(name string, currentType string, currentRange ast.Range) bool {
	if _, ok := v.mapImport[name]; ok && currentType != "import" {
		v.addErrorAtRange(fmt.Sprintf("name '%s' conflicts with an import alias", name), currentRange)
		return true
	}
	if _, ok := v.mapResource[name]; ok && currentType != "resource" {
		v.addErrorAtRange(fmt.Sprintf("name '%s' conflicts with a resource name", name), currentRange)
		return true
	}
	if _, ok := v.mapStep[name]; ok && currentType != "step" {
		v.addErrorAtRange(fmt.Sprintf("name '%s' conflicts with a step name", name), currentRange)
		return true
	}
	if _, ok := v.mapHandler[name]; ok && currentType != "handler" {
		v.addErrorAtRange(fmt.Sprintf("name '%s' conflicts with a handler name", name), currentRange)
		return true
	}
	if _, ok := v.mapWorkflow[name]; ok && currentType != "workflow" {
		v.addErrorAtRange(fmt.Sprintf("name '%s' conflicts with a workflow name", name), currentRange)
		return true
	}
	return false
}

func (v *Validator) registerDefinitions() {
	// Register Imports
	for i := v.program.ImportRefsStart; i < v.program.ImportRefsEnd; i++ {
		ref := v.ctx.ImportRefs[i]
		node := v.ctx.ImportNodes[ref]
		namespace := v.getImportNamespace(node)
		if namespace != "" {
			if _, ok := v.mapImport[namespace]; ok {
				v.addErrorAtRange(fmt.Sprintf("duplicate import alias: %s", namespace), v.ctx.ImportRanges[ref])
			} else {
				v.checkCollision(namespace, "import", v.ctx.ImportRanges[ref])
			}
			v.mapImport[namespace] = ref
		}
	}

	// Register Resources
	for i := v.program.ResourceRefsStart; i < v.program.ResourceRefsEnd; i++ {
		ref := v.ctx.ResourceRefs[i]
		node := v.ctx.ResourceNodes[ref]
		name := v.ctx.GetString(node.NameRef)
		if _, ok := v.mapResource[name]; ok {
			v.addErrorAtRange(fmt.Sprintf("duplicate resource definition: %s", name), v.ctx.ResourceRanges[ref])
		} else {
			v.checkCollision(name, "resource", v.ctx.ResourceRanges[ref])
		}
		v.mapResource[name] = ref
	}

	// Register Steps
	for i := v.program.StepRefsStart; i < v.program.StepRefsEnd; i++ {
		ref := v.ctx.StepRefs[i]
		node := v.ctx.StepBindingNodes[ref]
		name := v.ctx.GetString(node.NameRef)
		if _, ok := v.mapStep[name]; ok {
			v.addErrorAtRange(fmt.Sprintf("duplicate step definition: %s", name), v.ctx.StepRanges[ref])
		} else {
			v.checkCollision(name, "step", v.ctx.StepRanges[ref])
		}
		v.mapStep[name] = ref
	}

	// Register Handlers
	for i := v.program.HandlerRefsStart; i < v.program.HandlerRefsEnd; i++ {
		ref := v.ctx.HandlerRefs[i]
		node := v.ctx.HandlerNodes[ref]
		name := v.ctx.GetString(node.NameRef)
		if _, ok := v.mapHandler[name]; ok {
			v.addErrorAtRange(fmt.Sprintf("duplicate handler definition: %s", name), v.ctx.HandlerRanges[ref])
		} else {
			v.checkCollision(name, "handler", v.ctx.HandlerRanges[ref])
		}
		v.mapHandler[name] = ref
	}

	// Register Workflows
	for i := v.program.WorkflowRefsStart; i < v.program.WorkflowRefsEnd; i++ {
		ref := v.ctx.WorkflowRefs[i]
		node := v.ctx.WorkflowNodes[ref]
		name := v.ctx.GetString(node.NameRef)
		if _, ok := v.mapWorkflow[name]; ok {
			v.addErrorAtRange(fmt.Sprintf("duplicate workflow definition: %s", name), v.ctx.WorkflowRanges[ref])
		} else {
			v.checkCollision(name, "workflow", v.ctx.WorkflowRanges[ref])
		}
		v.mapWorkflow[name] = ref
	}
}

func (v *Validator) validateReferencesAll() {
	// Validate Resource references in Step bindings
	for i := v.program.StepRefsStart; i < v.program.StepRefsEnd; i++ {
		ref := v.ctx.StepRefs[i]
		node := v.ctx.StepBindingNodes[ref]
		if err := v.validateFunctionRef(node.FunctionRef); err != nil {
			v.addErrorAtRange(err.Error(), v.ctx.StepRanges[ref])
		}
	}

	// Validate Workflow references
	for i := v.program.WorkflowRefsStart; i < v.program.WorkflowRefsEnd; i++ {
		ref := v.ctx.WorkflowRefs[i]
		node := v.ctx.WorkflowNodes[ref]
		v.validateWorkflowReferencesAll(ref, node)
	}

	// Validate Handler references
	for i := v.program.HandlerRefsStart; i < v.program.HandlerRefsEnd; i++ {
		ref := v.ctx.HandlerRefs[i]
		node := v.ctx.HandlerNodes[ref]
		v.validateHandlerReferencesAll(ref, node)
	}
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
			v.usedResources[resourceName] = true
		}
	}
	return nil
}

func (v *Validator) validateWorkflowReferencesAll(ref ast.NodeRef, wd ast.WorkflowNode) {
	if wd.TrapRef.Start != wd.TrapRef.End {
		name := v.ctx.GetString(wd.TrapRef)
		if _, ok := v.mapHandler[name]; !ok {
			v.addErrorAtRange(fmt.Sprintf("undefined handler '%s' in workflow '%s'", name, v.ctx.GetString(wd.NameRef)), v.ctx.WorkflowRanges[ref])
		}
		v.usedHandlers[name] = true
	}

	for i := wd.StatementRefsStart; i < wd.StatementRefsEnd; i++ {
		psRef := v.ctx.StatementRefs[i]
		ps := v.ctx.PipelineStatementNodes[psRef]
		v.validatePipelineReferencesAll(ps)
	}
}

func (v *Validator) validateHandlerReferencesAll(ref ast.NodeRef, hd ast.HandlerNode) {
	for i := hd.HandlerStatementRefsStart; i < hd.HandlerStatementRefsEnd; i++ {
		hsRef := v.ctx.HandlerStatementRefs[i]
		hs := v.ctx.HandlerStatementNodes[hsRef]
		ps := v.ctx.PipelineStatementNodes[hs.StmtRef]
		v.validatePipelineReferencesAll(ps)
	}
}

func (v *Validator) validatePipelineReferencesAll(ps ast.PipelineStatementNode) {
	ref := ps.ExprRef
	if ref == 0 {
		return
	}

	// Register assignment if present
	if ps.AssignmentRef.Start != ps.AssignmentRef.End {
		name := v.ctx.GetString(ps.AssignmentRef)
		if _, ok := v.mapStep[name]; ok {
			v.addErrorAtRange(fmt.Sprintf("variable name '%s' conflicts with a step name", name), v.ctx.CallRanges[ref])
		}
		v.assignments[name] = ps.ExprRef
	}

	if int(ref) < len(v.ctx.PipeChainNodes) {
		pc := v.ctx.PipeChainNodes[ref]
		if pc.CallRefsEnd > pc.CallRefsStart {
			var lastOutputSchema []schema.ColumnSchema
			for i := pc.CallRefsStart; i < pc.CallRefsEnd; i++ {
				callRef := v.ctx.CallRefs[i]
				call := v.ctx.CallNodes[callRef]
				v.validateCallReferencesAll(callRef, call, i == pc.CallRefsStart)

				// Type Checking
				currentSchemas, err := v.resolveCallSchemas(call, lastOutputSchema)
				if err != nil {
					v.addErrorAtRange(err.Error(), v.ctx.CallRanges[callRef])
				} else if currentSchemas != nil {
					if len(currentSchemas.Input) > 0 {
						if err := schema.Compatible(lastOutputSchema, currentSchemas.Input); err != nil {
							v.addErrorAtRange(fmt.Sprintf("Type mismatch: %v", err), v.ctx.CallRanges[callRef])
						}
					}
					lastOutputSchema = currentSchemas.Output
				}
			}
		}
	}
}

func (v *Validator) resolveModulePath(alias string) string {
	if ref, ok := v.mapImport[alias]; ok {
		node := v.ctx.ImportNodes[ref]
		path := v.ctx.GetString(node.PathRef)
		if len(path) >= 2 && path[0] == '"' && path[len(path)-1] == '"' {
			path = path[1 : len(path)-1]
		}
		return path
	}
	return alias
}

func (v *Validator) resolveCallSchemas(call ast.CallNode, incomingSchema []schema.ColumnSchema) (*schema.StepSchemas, error) {
	if call.IsPrql {
		// TODO: Infer PRQL schemas if possible, or assume it passes through for now
		return nil, nil
	}

	var stepName string
	var fnRef ast.NodeRef
	if call.FunctionRef != 0 {
		fnRef = call.FunctionRef
		fn := v.ctx.FunctionRefNodes[fnRef]
		if fn.ModuleRef.Start != fn.ModuleRef.End {
			moduleName := v.ctx.GetString(fn.ModuleRef)
			v.usedImports[moduleName] = true
			fullModule := v.resolveModulePath(moduleName)
			stepName = fmt.Sprintf("%s.%s", fullModule, v.ctx.GetString(fn.NameRef))
		} else {
			stepName = v.ctx.GetString(fn.NameRef)
		}
	} else if call.NameRef.Start != call.NameRef.End {
		stepName = v.ctx.GetString(call.NameRef)
	}

	if stepName == "" {
		return nil, nil
	}

	// If it's a bound step, we need to resolve what it's bound to
	if boundRef, ok := v.mapStep[stepName]; ok {
		boundNode := v.ctx.StepBindingNodes[boundRef]
		fnRef = boundNode.FunctionRef
		fn := v.ctx.FunctionRefNodes[fnRef]
		if fn.ModuleRef.Start != fn.ModuleRef.End {
			moduleName := v.ctx.GetString(fn.ModuleRef)
			fullModule := v.resolveModulePath(moduleName)
			stepName = fmt.Sprintf("%s.%s", fullModule, v.ctx.GetString(fn.NameRef))
		} else {
			stepName = v.ctx.GetString(fn.NameRef)
		}
	}

	if resolver, ok := intrinsicResolvers[stepName]; ok {
		return resolver(call, incomingSchema, v, fnRef)
	}

	if v.schemas != nil {
		if s, ok := v.schemas[stepName]; ok {
			return &s, nil
		}
	}
	return nil, nil
}

var intrinsicResolvers = map[string]func(call ast.CallNode, in []schema.ColumnSchema, v *Validator, fnRef ast.NodeRef) (*schema.StepSchemas, error){
	"std/col.cast": resolveCastSchema,
	"std/io.print": resolvePrintSchema,
}

func resolvePrintSchema(call ast.CallNode, in []schema.ColumnSchema, v *Validator, fnRef ast.NodeRef) (*schema.StepSchemas, error) {
	for _, col := range in {
		if !isPrintableType(col.ArrowType) {
			return nil, fmt.Errorf("print: column '%s' has unprintable type '%s'", col.Name, col.ArrowType)
		}
	}
	return &schema.StepSchemas{
		Input:  in,
		Output: nil,
	}, nil
}

func isPrintableType(t string) bool {
	switch strings.ToLower(t) {
	case "int8", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64",
		"float32", "float64",
		"string", "utf8", "text",
		"bool", "boolean":
		return true
	default:
		return false
	}
}

func normalizeArrowType(t string) (string, error) {
	switch strings.ToLower(t) {
	case "int8":
		return "int8", nil
	case "int16":
		return "int16", nil
	case "int32":
		return "int32", nil
	case "int64":
		return "int64", nil
	case "uint8":
		return "uint8", nil
	case "uint16":
		return "uint16", nil
	case "uint32":
		return "uint32", nil
	case "uint64":
		return "uint64", nil
	case "float32":
		return "float32", nil
	case "float64":
		return "float64", nil
	case "string", "utf8", "text":
		return "utf8", nil
	case "bool", "boolean":
		return "bool", nil
	default:
		return "", fmt.Errorf("unsupported type: %s", t)
	}
}

func isCastAllowed(from, to string) bool {
	_, err1 := normalizeArrowType(from)
	_, err2 := normalizeArrowType(to)
	return err1 == nil && err2 == nil
}

func resolveCastSchema(call ast.CallNode, in []schema.ColumnSchema, v *Validator, fnRef ast.NodeRef) (*schema.StepSchemas, error) {
	var configRef ast.NodeRef
	if fnRef != 0 {
		fn := v.ctx.FunctionRefNodes[fnRef]
		configRef = fn.ConfigRef
	}
	cfg, err := v.parseDict(configRef)
	if err != nil {
		return nil, fmt.Errorf("cast: failed to parse config: %w", err)
	}

	var globalTo string
	if toVal, ok := cfg["to"]; ok {
		if s, ok := toVal.(string); ok {
			globalTo = s
		}
	}

	columnsMap := make(map[string]string)
	if colsVal, ok := cfg["columns"]; ok {
		if m, ok := colsVal.(map[string]any); ok {
			for k, val := range m {
				if s, ok := val.(string); ok {
					columnsMap[k] = s
				}
			}
		}
	}

	if len(columnsMap) == 0 && globalTo == "" {
		return nil, fmt.Errorf("cast: config must specify either 'columns' map or global 'to' type")
	}

	out := make([]schema.ColumnSchema, len(in))
	for i, col := range in {
		out[i] = col
		var targetTypeStr string
		if globalTo != "" {
			targetTypeStr = globalTo
		} else if t, ok := columnsMap[col.Name]; ok {
			targetTypeStr = t
		}

		if targetTypeStr != "" {
			norm, err := normalizeArrowType(targetTypeStr)
			if err != nil {
				return nil, fmt.Errorf("cast: invalid target type '%s' for column '%s': %w", targetTypeStr, col.Name, err)
			}
			if !isCastAllowed(col.ArrowType, norm) {
				return nil, fmt.Errorf("cast: conversion from '%s' to '%s' is not supported for column '%s'", col.ArrowType, norm, col.Name)
			}
			out[i].ArrowType = norm
		}
	}

	return &schema.StepSchemas{
		Input:  in,
		Output: out,
	}, nil
}

func (v *Validator) parseLiteral(ref ast.NodeRef) (any, error) {
	if ref == ast.NilNode {
		return nil, nil
	}

	node := v.ctx.LiteralNodes[ref]
	switch node.Type {
	case ast.LiteralString:
		return v.ctx.GetString(node.ValueRef), nil
	case ast.LiteralInt:
		val, err := strconv.ParseInt(v.ctx.GetString(node.ValueRef), 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case ast.LiteralFloat:
		val, err := strconv.ParseFloat(v.ctx.GetString(node.ValueRef), 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case ast.LiteralBool:
		return v.ctx.GetString(node.ValueRef) == "true", nil
	case ast.LiteralNull:
		return nil, nil
	case ast.LiteralDict:
		return v.parseDict(node.Ref)
	case ast.LiteralList:
		listNode := v.ctx.ListNodes[node.Ref]
		result := make([]any, 0)
		for i := listNode.LiteralRefsStart; i < listNode.LiteralRefsEnd; i++ {
			val, err := v.parseLiteral(v.ctx.LiteralRefs[i])
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

func (v *Validator) parseDict(ref ast.NodeRef) (map[string]any, error) {
	if ref == ast.NilNode {
		return make(map[string]any), nil
	}

	dictNode := v.ctx.DictNodes[ref]
	result := make(map[string]any)

	for i := dictNode.PairRefsStart; i < dictNode.PairRefsEnd; i++ {
		pairRef := v.ctx.PairRefs[i]
		pair := v.ctx.PairNodes[pairRef]
		key := v.ctx.GetString(pair.KeyRef)
		val, err := v.parseLiteral(pair.ValueRef)
		if err != nil {
			return nil, err
		}
		result[key] = val
	}

	return result, nil
}

func (v *Validator) validateCallReferencesAll(ref ast.NodeRef, call ast.CallNode, isFirstInChain bool) {
	if call.TrapRef.Start != call.TrapRef.End {
		name := v.ctx.GetString(call.TrapRef)
		if _, ok := v.mapHandler[name]; !ok {
			v.addErrorAtRange(fmt.Sprintf("undefined handler '%s' in step call", name), v.ctx.CallRanges[ref])
		}
		v.usedHandlers[name] = true
	}

	if !call.IsPrql {
		name := v.ctx.GetString(call.NameRef)
		if name != "" {
			_, isStep := v.mapStep[name]
			_, isVar := v.assignments[name]

			if isStep {
				v.usedSteps[name] = true
			} else if isVar {
				v.usedVariables[name] = true
			} else {
				// undefined reference
				v.addErrorAtRange(fmt.Sprintf("undefined step or variable: %s", name), v.ctx.CallRanges[ref])
			}
		}
	} else {
		// Validate PRQL query syntax, referenced assignments and potential conflicts
		query := v.ctx.GetString(call.QueryRef)
		cleaned := cleanPRQL(query)
		tables, aliases, err := ParsePRQLTables(cleaned)
		if err != nil {
			v.addErrorAtRange(err.Error(), v.ctx.CallRanges[ref])
		} else {
			for _, tbl := range tables {
				if tbl == "input" {
					if isFirstInChain {
						v.addErrorAtRange("cannot use 'input' table at the start of a pipeline; it can only be used when PRQL is piped after a '|' operator", v.ctx.CallRanges[ref])
					}
					continue
				}
				// Verify the assignment exists in the workflow scope
				if _, ok := v.assignments[tbl]; !ok {
					v.addErrorAtRange(fmt.Sprintf("undefined assignment referenced in PRQL: %s", tbl), v.ctx.CallRanges[ref])
				} else {
					v.usedVariables[tbl] = true
				}
			}
			for _, aliasName := range aliases {
				// Verify the alias does not conflict with existing Heddle assignments
				if _, ok := v.assignments[aliasName]; ok {
					v.addErrorAtRange(fmt.Sprintf("naming conflict: PRQL alias '%s' conflicts with Heddle assignment", aliasName), v.ctx.CallRanges[ref])
				}
			}
		}
	}
}

func (v *Validator) checkUnused() {
	// Check unused imports
	for i := v.program.ImportRefsStart; i < v.program.ImportRefsEnd; i++ {
		ref := v.ctx.ImportRefs[i]
		node := v.ctx.ImportNodes[ref]
		var name string
		if node.AliasRef.Start != node.AliasRef.End {
			name = v.ctx.GetString(node.AliasRef)
		} else {
			name = v.ctx.GetString(node.PathRef)
		}
		if !v.usedImports[name] {
			// For now, let's keep imports warning commented as it might be noisy
			// if the user is in the middle of typing.
			// But for "static analysis" it should be there.
			// v.addWarning(fmt.Sprintf("unused import: %s", name), 0, 0, TagUnnecessary)
		}
	}
	// Check unused resources
	for name, ref := range v.mapResource {
		if !v.usedResources[name] {
			v.addWarningAtRange(fmt.Sprintf("unused resource: %s", name), v.ctx.ResourceRanges[ref], TagUnnecessary)
		}
	}

	// Check unused steps
	for name, ref := range v.mapStep {
		if !v.usedSteps[name] {
			v.addWarningAtRange(fmt.Sprintf("unused step: %s", name), v.ctx.StepRanges[ref], TagUnnecessary)
		}
	}

	// Check unused handlers
	for name, ref := range v.mapHandler {
		if !v.usedHandlers[name] {
			v.addWarningAtRange(fmt.Sprintf("unused handler: %s", name), v.ctx.HandlerRanges[ref], TagUnnecessary)
		}
	}

	// Check unused variables
	for name := range v.assignments {
		if !v.usedVariables[name] {
			// v.addWarning(fmt.Sprintf("unused variable: %s", name), 0, 0, TagUnnecessary)
		}
	}
}

func (v *Validator) detectCyclesAll() {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for name := range v.mapStep {
		if v.hasCycle(name, visited, recStack) {
			// Errors are added inside hasCycle
		}
	}
}

func (v *Validator) hasCycle(name string, visited, recStack map[string]bool) bool {
	if recStack[name] {
		ref := v.mapStep[name]
		v.addErrorAtRange(fmt.Sprintf("recursive step definition detected: %s", name), v.ctx.StepRanges[ref])
		return true
	}
	if visited[name] {
		return false
	}

	visited[name] = true
	recStack[name] = true

	ref := v.mapStep[name]
	node := v.ctx.StepBindingNodes[ref]
	fn := v.ctx.FunctionRefNodes[node.FunctionRef]

	if fn.ModuleRef.Start == fn.ModuleRef.End {
		// Local step reference
		nextName := v.ctx.GetString(fn.NameRef)
		if _, ok := v.mapStep[nextName]; ok {
			if v.hasCycle(nextName, visited, recStack) {
				recStack[name] = false
				return true
			}
		}
	}

	recStack[name] = false
	return false
}

func (v *Validator) detectCyclesInWorkflow(wfRef ast.NodeRef, wd ast.WorkflowNode) {
	// For workflows, we could detect data flow cycles if we track variable dependencies
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
