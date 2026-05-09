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

func TestLowerer_BasicWorkflow(t *testing.T) {
	input := `
import "std/io" io
import "func/some" some

resource db = pg.connection {
  host: "pg.internal"
}

resource db = pg.connection_default

step extract = <conn=db> pg.query {
  query: "SELECT *"
}

step transform = some.func

step print = io.print

workflow main {
  extract
    | transform
    | print
  > output

  output
    | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	lowerer := NewLowerer(ctx)
	programIR, err := lowerer.Lower(prog)
	require.NoError(t, err)

	// Verify ProgramIR
	assert.NotNil(t, programIR)
	assert.Equal(t, 1, len(programIR.Workflows))

	// Get the workflow instruction
	wfID := programIR.Workflows[0]
	wfInst, ok := programIR.Instructions[wfID].(*ir.FlowInstruction)
	require.True(t, ok)
	assert.Equal(t, "main", wfInst.Name)
	require.Equal(t, 1, len(wfInst.Heads))

	// Follow the chain
	headID := wfInst.Heads[0]
	step1, ok := programIR.Instructions[headID].(*ir.StepInstruction)
	require.True(t, ok)
	assert.Equal(t, "extract", step1.DefinitionName)
	assert.Equal(t, "db", step1.Resources["conn"])

	step2ID := step1.Next
	require.NotEmpty(t, step2ID)
	step2, ok := programIR.Instructions[step2ID].(*ir.StepInstruction)
	require.True(t, ok)
	assert.Equal(t, "transform", step2.DefinitionName)

	step3ID := step2.Next
	require.NotEmpty(t, step3ID)
	step3, ok := programIR.Instructions[step3ID].(*ir.StepInstruction)
	require.True(t, ok)
	assert.Equal(t, "io.print", step3.DefinitionName)
	assert.Equal(t, "output", step3.Assignment)
	assert.Empty(t, step3.Next)
}

func TestLowerer_ComplexWorkflow(t *testing.T) {
	input := `
import "fhub/kafka" kafka
import "fhub/postgresql" pg
import "fhub/clickhouse" ch
import "fhub/llm" openai
import "fraud-score/detect" fraud_detection

// 1. Centralized Resources (State/Connections)
// PostgreSQL
resource pg_db = pg.connection {
  host: "pg.internal"
} 

// Clickhouse
resource ch_db = ch.connection {
  host: "ch.internal"
}

// Kafka
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

step produce_dlq = <broker=kf_broker> kafka.produce {
  topic: "dlq_alerts"
}

step consume_live_transactions = <broker=kf_broker> kafka.consume {
  topic: "live_transactions"
}

step producer_fraud_audits = <broker=kf_broker> kafka.produce {
  topic: "fraud_audits"
}

// Global error catcher
handler alert_on_fail {
  *
    | produce_dlq
}

// Step error catcher
handler alert_step_fail {
  *
    | produce_dlq
}


// 3. Strict DAG Workflow
workflow FraudDetection ? alert_on_fail {

  consume_live_transactions
  > tx_stream

  // 1. Filter: High-value txns isolated via native PRQL
  // 2. Process: Imperative logic with localized error trap
  // 3. Enrich: Joined with user data & risk metrics
  // 4. Audit: LLM generates natural language report
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
    | producer_fraud_audits
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	lowerer := NewLowerer(ctx)
	programIR, err := lowerer.Lower(prog)
	require.NoError(t, err)

	assert.Equal(t, 1, len(programIR.Workflows))
	wfID := programIR.Workflows[0]
	wfInst := programIR.Instructions[wfID].(*ir.FlowInstruction)
	assert.Equal(t, "FraudDetection", wfInst.Name)
	assert.Equal(t, 1, len(wfInst.Heads))
}

func TestLowerer_HandlersAndTraps(t *testing.T) {
	input := `
handler err_log {
  *
    | io.log_error
}

workflow main ? err_log {
  fetch ? err_log
    | process
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	lowerer := NewLowerer(ctx)
	programIR, err := lowerer.Lower(prog)
	require.NoError(t, err)

	// Verify Handlers
	var handlerID string
	for id, inst := range programIR.Instructions {
		if flow, ok := inst.(*ir.FlowInstruction); ok && flow.Name == "err_log" {
			handlerID = id
			break
		}
	}
	require.NotEmpty(t, handlerID)

	// Verify Workflow Trap
	wfID := programIR.Workflows[0]
	wfInst := programIR.Instructions[wfID].(*ir.FlowInstruction)
	assert.Equal(t, handlerID, wfInst.Handler)

	// Verify Step Trap
	headID := wfInst.Heads[0]
	step1 := programIR.Instructions[headID].(*ir.StepInstruction)
	assert.Equal(t, "fetch", step1.DefinitionName)
	assert.Equal(t, handlerID, step1.Handler)
}

func TestLowerer_PRQL(t *testing.T) {
	input := `
workflow main {
  (
    from users
    select {id, name}
  )
  | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	lowerer := NewLowerer(ctx)
	programIR, err := lowerer.Lower(prog)
	require.NoError(t, err)

	wfID := programIR.Workflows[0]
	wfInst := programIR.Instructions[wfID].(*ir.FlowInstruction)
	headID := wfInst.Heads[0]
	step1 := programIR.Instructions[headID].(*ir.StepInstruction)

	assert.Equal(t, "prql", step1.DefinitionName)
	assert.Contains(t, step1.Config["query"].(string), "from users")
}
