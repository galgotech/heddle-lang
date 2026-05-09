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

	// Find handler step
	handlerStepID := flow.Handler
	handlerStep := irProg.Instructions[handlerStepID].(*ir.StepInstruction)
	assert.Equal(t, []string{"std/io", "stderr"}, handlerStep.Call)
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
			// Verify handler content
			hStep := irProg.Instructions[step.Handler].(*ir.StepInstruction)
			assert.Equal(t, "produce_dead_letter_queue", hStep.DefinitionName)
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

		currID = step.Next
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
		curr = irProg.Instructions[curr].(*ir.StepInstruction).Next
	}
	assert.Equal(t, 3, count)
}
