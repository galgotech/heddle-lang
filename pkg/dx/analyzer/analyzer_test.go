package analyzer

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzer_IsStepDefined(t *testing.T) {
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	reg := locality.NewDataLocalityRegistry()
	a := New(ctx, reg)

	// 1. Defined in StepBindings
	stepName := "my-step"
	nameRef := ctx.AddString(stepName)
	ctx.AddStepBindingNode(ast.StepBindingNode{
		NameRef: nameRef,
	})

	assert.True(t, a.isStepDefined(stepName))

	// 2. Defined in Registry
	regStep := "external-step"
	err := reg.Put(locality.NewMetadata("", regStep, locality.Output, map[string]string{}))
	assert.NoError(t, err)

	assert.True(t, a.isStepDefined(regStep))

	// 3. Undefined
	assert.False(t, a.isStepDefined("unknown-step"))
}

func TestAnalyzer_Analyze_UndefinedStep(t *testing.T) {
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	reg := locality.NewDataLocalityRegistry()
	a := New(ctx, reg)

	// Create a program with a call to an undefined step
	unknownStep := "unknown-step"
	nameRef := ctx.AddString(unknownStep)
	
	callRef := ctx.AddCallNode(ast.CallNode{
		NameRef: nameRef,
	})
	ctx.CallRefs = append(ctx.CallRefs, callRef)
	
	chainRef := ctx.AddPipeChainNode(ast.PipeChainNode{
		CallRefsStart: 0,
		CallRefsEnd:   1,
	})
	
	stmtRef := ctx.AddPipelineStatementNode(ast.PipelineStatementNode{
		ExprRef: chainRef,
	})
	ctx.StatementRefs = append(ctx.StatementRefs, stmtRef)
	
	wfRef := ctx.AddWorkflowNode(ast.WorkflowNode{
		NameRef:            ctx.AddString("test-wf"),
		StatementRefsStart: 0,
		StatementRefsEnd:   1,
	})
	ctx.WorkflowRefs = append(ctx.WorkflowRefs, wfRef)
	
	program := ast.ProgramNode{
		WorkflowRefsStart: 0,
		WorkflowRefsEnd:   1,
	}

	diagnostics := a.Analyze(program)

	assert.Len(t, diagnostics, 1)
	assert.Contains(t, diagnostics[0].Message, "Step 'unknown-step' is not defined")
}
