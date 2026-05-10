package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestLowerer_Basic(t *testing.T) {
	input := `
import "std/io" io

resource pg_db = pg.connection {
  host: "localhost"
}

step fetch_users = <connection=pg_db> pg.query {
  query: "SELECT * FROM users"
}

workflow main {
  fetch_users
    | io.stdout
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)

	require.NoError(t, err)
	require.NotNil(t, irProg)

	// Verify Resources
	var pgRes *ir.ResourceInstruction
	for _, inst := range irProg.Instructions {
		if r, ok := inst.(*ir.ResourceInstruction); ok && r.Name == "pg_db" {
			pgRes = r
			break
		}
	}
	require.NotNil(t, pgRes, "Resource 'pg_db' not found in IR")
	assert.Equal(t, []string{"pg", "connection"}, pgRes.Provider)
	assert.Equal(t, "localhost", pgRes.Config["host"])

	// Verify Steps
	var fetchStep *ir.StepInstruction
	for _, inst := range irProg.Instructions {
		if s, ok := inst.(*ir.StepInstruction); ok && s.DefinitionName == "fetch_users" {
			fetchStep = s
			break
		}
	}
	require.NotNil(t, fetchStep, "Step 'fetch_users' not found in IR")
	assert.Equal(t, []string{"pg", "query"}, fetchStep.Call)
	assert.Equal(t, "pg_db", fetchStep.Resources["connection"])

	// Verify Workflow
	assert.Len(t, irProg.Workflows, 1)
	flowID := irProg.Workflows[0]
	flow := irProg.Instructions[flowID].(*ir.FlowInstruction)
	assert.Equal(t, "main", flow.Name)
	assert.Len(t, flow.Heads, 1)
}

func TestLowerer_Handler(t *testing.T) {
	input := `
import "std/io" io

handler on_error {
  *
    | io.stderr
}

workflow main ? on_error {
  (from input)
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)

	require.NoError(t, err)

	// Find workflow
	var flow *ir.FlowInstruction
	for _, inst := range irProg.Instructions {
		if f, ok := inst.(*ir.FlowInstruction); ok && f.Name == "main" {
			flow = f
			break
		}
	}
	require.NotNil(t, flow)
	require.NotEmpty(t, flow.Handler, "Workflow handler ID should not be empty")

	// Find handler step (should be the implicit identity step from '*')
	handlerStepID := flow.Handler
	handlerStep := irProg.Instructions[handlerStepID].(*ir.StepInstruction)
	assert.Equal(t, "identity", handlerStep.DefinitionName)
	assert.Equal(t, []string{"std", "identity"}, handlerStep.Call)
	assert.True(t, handlerStep.Config["is_catch_all"].(bool))

	// Next step in handler should be io.stderr
	require.NotEmpty(t, handlerStep.Next)
	stderrStep := irProg.Instructions[handlerStep.Next[0]].(*ir.StepInstruction)
	assert.Equal(t, []string{"std/io", "stderr"}, stderrStep.Call)
}

func TestLowerer_FraudDetection(t *testing.T) {
	input := `
import "fhub/kafka" kafka
import "fhub/postgresql" pg
import "fhub/clickhouse" ch
import "fhub/llm" openai
import "fraud-score/detect" fraud_detection

// 1. Centralized Resources (State/Connections)
resource pg_db = pg.connection {
  host: "pg.internal"
} 

resource ch_db = ch.connection {
  host: "ch.internal"
}

resource kf_broker = kafka.connection {
  broker: "kafka.internal:9092"
}

// 2. Bound Imperative Steps with Resource Injection
step fetch_user_data = <connection=pg_db> pg.query {
  query: "SELECT id AS user_id, country FROM users WHERE id = @user_id"
}

step fetch_risk_profile = <connection=ch_db> ch.query {
  query: "SELECT user_id, velocity_score FROM risk_metrics WHERE user_id = @user_id"
}

step generate_audit = openai.prompt {
  system: "Analyze transaction, location, and velocity score. Generate a fraud audit text report."
}

step fetch_transactions = <broker=kf_broker> kafka.consume {
  topic: "live_transactions"
}

step produce_fraud_audits = <broker=kf_broker> kafka.produce {
  topic: "fraud_audits"
}

step produce_dead_letter_queue = <broker=kf_broker> kafka.produce {
  topic: "dlq_alerts"
}

// Global error catcher
handler alert_on_fail {
  *
    | produce_dead_letter_queue
}

// Step error catcher
handler alert_step_fail {
  *
    | produce_dead_letter_queue
}

// 3. Strict DAG Workflow
workflow FraudDetection ? alert_on_fail {
  fetch_transactions
  > tx_stream

  tx_stream
    | (
        from input
        filter amount > 10000
        select {user_id, amount}
      ) 
    | fraud_detection.process ? alert_step_fail
    | (
        from input
        join fetch_user_data (==user_id)
        join fetch_risk_profile (==user_id)
      )
    | generate_audit
    | produce_fraud_audits
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)
	require.NoError(t, err)

	// 1. Verify Imports (Resolved correctly)
	foundKafka := false
	for _, inst := range irProg.Instructions {
		if imp, ok := inst.(*ir.ImportInstruction); ok && imp.Alias == "kafka" {
			assert.Equal(t, "fhub/kafka", imp.Path)
			foundKafka = true
		}
	}
	assert.True(t, foundKafka, "Import 'kafka' not found")

	// 2. Verify Resources (Correct providers and configs)
	var kfRes *ir.ResourceInstruction
	for _, inst := range irProg.Instructions {
		if r, ok := inst.(*ir.ResourceInstruction); ok && r.Name == "kf_broker" {
			kfRes = r
			break
		}
	}
	require.NotNil(t, kfRes)
	assert.Equal(t, []string{"fhub/kafka", "connection"}, kfRes.Provider)
	assert.Equal(t, "kafka.internal:9092", kfRes.Config["broker"])

	// 3. Verify Workflow & DAG Chaining
	var flow *ir.FlowInstruction
	for _, inst := range irProg.Instructions {
		if f, ok := inst.(*ir.FlowInstruction); ok && f.Name == "FraudDetection" {
			flow = f
			break
		}
	}
	require.NotNil(t, flow)
	assert.NotEmpty(t, flow.Handler, "Flow should have a handler linked")

	// Traverse the entire chain
	currID := flow.Heads[0]
	sequence := []struct {
		Name     string
		Call     []string
		Handler  bool
		Assign   string
		Resource string
	}{
		{Name: "fetch_transactions", Call: []string{"fhub/kafka", "consume"}, Assign: "tx_stream", Resource: "kf_broker"},
		{Name: "query", Call: []string{"std", "query"}},
		{Name: "", Call: []string{"fraud-score/detect", "process"}, Handler: true},
		{Name: "query", Call: []string{"std", "query"}},
		{Name: "generate_audit", Call: []string{"fhub/llm", "prompt"}},
		{Name: "produce_fraud_audits", Call: []string{"fhub/kafka", "produce"}},
	}

	for i, expected := range sequence {
		require.NotEmpty(t, currID, "DAG ended prematurely at step %d", i)
		step := irProg.Instructions[currID].(*ir.StepInstruction)

		if expected.Name != "" {
			assert.Equal(t, expected.Name, step.DefinitionName, "Step %d name mismatch", i)
		}
		assert.Equal(t, expected.Call, step.Call, "Step %d call mismatch", i)

		if expected.Handler {
			assert.NotEmpty(t, step.Handler, "Step %d should have a handler", i)
			// Verify handler content (starts with identity step '*')
			hStep := irProg.Instructions[step.Handler].(*ir.StepInstruction)
			assert.Equal(t, "identity", hStep.DefinitionName)
			require.NotEmpty(t, hStep.Next)
			nextHStep := irProg.Instructions[hStep.Next[0]].(*ir.StepInstruction)
			assert.Equal(t, "produce_dead_letter_queue", nextHStep.DefinitionName)
		}

		if expected.Assign != "" {
			assert.Equal(t, expected.Assign, step.Assignment, "Step %d assignment mismatch", i)
		}

		if expected.Resource != "" {
			found := false
			for _, r := range step.Resources {
				if r == expected.Resource {
					found = true
					break
				}
			}
			assert.True(t, found, "Step %d missing resource %s", i, expected.Resource)
		}

		if len(step.Next) > 0 {
			currID = step.Next[0]
		} else {
			currID = ""
		}
	}
	assert.Empty(t, currID, "DAG has extra steps at the end")
}

func TestLowerer_DAGTraversal(t *testing.T) {
	// Simple traversal test already covered in TestLowerer_FraudDetection,
	// but keeping a minimal one for quick regression.
	input := `
import "std/io" io

step a = io.log {
  msg: "a"
}
step b = io.log {
  msg: "b"
}
step c = io.log {
  msg: "c"
}

workflow main {
  a
    | b
    | c
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)
	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	low := NewLowerer(ctx)
	irProg, _ := low.Lower(prog)

	flowID := irProg.Workflows[0]
	flow := irProg.Instructions[flowID].(*ir.FlowInstruction)

	curr := flow.Heads[0]
	count := 0
	for curr != "" {
		count++
		step := irProg.Instructions[curr].(*ir.StepInstruction)
		if len(step.Next) > 0 {
			curr = step.Next[0]
		} else {
			curr = ""
		}
	}
	assert.Equal(t, 3, count)
}

func TestLowerer_ComplexWorkflowAssignment(t *testing.T) {
	input := `
import "std/m" m

step s1 = m.extract
step s2 = m.filter
step s3 = m.output
step s4 = m.search
step r1 = m.retry

handler recover {
  *
    | r1
}

handler recover_local {
  *
    | r1
}

workflow main ? recover {
  s1 ? recover_local
    | s2
  > pipe_assignment

  pipe_assignment
    | s1
    | s2
  > pipe_assignment_2

  (from pipe_assignment join pipe_assignment_2 select o1)
    | s3

  s4
    | (from input join pipe_assignment_2 select o1)
    | r1
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)
	require.NoError(t, err)

	// Find workflow
	var flow *ir.FlowInstruction
	for _, inst := range irProg.Instructions {
		if f, ok := inst.(*ir.FlowInstruction); ok && f.Name == "main" {
			flow = f
			break
		}
	}
	require.NotNil(t, flow)
	assert.NotEmpty(t, flow.Handler, "Workflow should have a handler linked")

	// With the new lowerer, we expect 3 heads because statements without resolved data dependencies
	// start in parallel.
	assert.Len(t, flow.Heads, 3)

	// Chain 1: s1 -> s2 (pipe_assignment) -> s1 -> s2 (pipe_assignment_2)
	currID := flow.Heads[0]
	chain1 := []string{"s1", "s2", "s1", "s2"}
	for _, name := range chain1 {
		require.NotEmpty(t, currID)
		step := irProg.Instructions[currID].(*ir.StepInstruction)
		assert.Equal(t, name, step.DefinitionName)
		if len(step.Next) > 0 {
			currID = step.Next[0]
		} else {
			currID = ""
		}
	}

	// Chain 2: query (Join) -> s3
	currID = flow.Heads[1]
	chain2 := []string{"query", "s3"}
	for _, name := range chain2 {
		require.NotEmpty(t, currID)
		step := irProg.Instructions[currID].(*ir.StepInstruction)
		assert.Equal(t, name, step.DefinitionName)
		if len(step.Next) > 0 {
			currID = step.Next[0]
		} else {
			currID = ""
		}
	}

	// Chain 3: s4 -> query -> r1
	currID = flow.Heads[2]
	chain3 := []string{"s4", "query", "r1"}
	for _, name := range chain3 {
		require.NotEmpty(t, currID)
		step := irProg.Instructions[currID].(*ir.StepInstruction)
		assert.Equal(t, name, step.DefinitionName)
		if len(step.Next) > 0 {
			currID = step.Next[0]
		} else {
			currID = ""
		}
	}
}

func TestLowerer_Dataframe(t *testing.T) {
	input := `
import "std/io" io

workflow main {
  [
    {
      "id": 1,
      "name": "Alice"
    },
    {
      "id": 2,
      "name": "Bob"
    }
  ]
    | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)
	require.NoError(t, err)

	// Verify workflow head is std.data
	flowID := irProg.Workflows[0]
	flow := irProg.Instructions[flowID].(*ir.FlowInstruction)
	require.Len(t, flow.Heads, 1)

	dataStepID := flow.Heads[0]
	dataStep := irProg.Instructions[dataStepID].(*ir.StepInstruction)
	assert.Equal(t, "data", dataStep.DefinitionName)
	assert.Equal(t, []string{"std", "data"}, dataStep.Call)

	// Verify data content
	data := dataStep.Config["data"].([]map[string]any)
	require.Len(t, data, 2)
	assert.Equal(t, int64(1), data[0]["id"])
	assert.Equal(t, "Alice", data[0]["name"])
	assert.Equal(t, int64(2), data[1]["id"])
	assert.Equal(t, "Bob", data[1]["name"])

	// Verify link to io.print
	require.Len(t, dataStep.Next, 1)
	printStepID := dataStep.Next[0]
	printStep := irProg.Instructions[printStepID].(*ir.StepInstruction)
	assert.Equal(t, []string{"std/io", "print"}, printStep.Call)
}

func TestLowerer_IdentityStep(t *testing.T) {
	input := `
import "std/io" io

step a = io.print
step b = io.print

handler test {
  *
    | io.print
}

workflow main ? test {
  a
    | b
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	progNode := p.Parse()
	require.Empty(t, p.Errors())

	low := NewLowerer(ctx)
	irProg, err := low.Lower(progNode)
	require.NoError(t, err)

	// Verify handler has an identity step at the head
	var handlerStep *ir.StepInstruction
	for _, inst := range irProg.Instructions {
		if s, ok := inst.(*ir.StepInstruction); ok && s.DefinitionName == "identity" {
			handlerStep = s
			break
		}
	}
	require.NotNil(t, handlerStep, "Identity step '*' not found in IR")
	assert.Equal(t, []string{"std", "identity"}, handlerStep.Call)

	// Verify identity step links to io.print
	require.NotEmpty(t, handlerStep.Next)
	nextStep := irProg.Instructions[handlerStep.Next[0]].(*ir.StepInstruction)
	assert.Equal(t, []string{"std/io", "print"}, nextStep.Call)

	// Verify main workflow is still linked correctly
	flowID := irProg.Workflows[0]
	flow := irProg.Instructions[flowID].(*ir.FlowInstruction)
	aID := flow.Heads[0]
	aStep := irProg.Instructions[aID].(*ir.StepInstruction)
	assert.Equal(t, "a", aStep.DefinitionName)
}
