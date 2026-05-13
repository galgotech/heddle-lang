package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

func runParserTest(t *testing.T, input string, expectedErrs int, check func(*testing.T, *ast.ASTContext, ast.ProgramNode)) {
	t.Helper()
	l := lexer.New(input)
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	p := New(l, ctx)
	program := p.Parse()

	errs := p.Errors()
	if len(errs) != expectedErrs {
		t.Fatalf("expected %d errors, got %d: %v", expectedErrs, len(errs), errs)
	}

	if check != nil {
		check(t, ctx, program)
	}
}

func TestFullFeatureWorkflow(t *testing.T) {
	input := `
import "std/io" io
import "db/pg" pg

resource res_pg = pg.connect {
  host: "localhost"
  port: 5432
}

step fetch_users = <connection=res_pg> pg.query {
  query: "SELECT * FROM users"
}

handler on_err {
  *
    | io.stderr
}

handler alert_step_fail {
  *
    | <broker=kf_broker> kafka.produce { topic: "dlq_alerts" }
}

workflow main ? on_err {
  fetch_users ? alert_step_fail
  > users

  users
    | (from input select {id, email})
    | io.print
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		// Verify Top-level definitions
		if (program.ImportRefsEnd - program.ImportRefsStart) != 2 {
			t.Errorf("expected 2 imports, got %d", program.ImportRefsEnd-program.ImportRefsStart)
		}
		if (program.ResourceRefsEnd - program.ResourceRefsStart) != 1 {
			t.Errorf("expected 1 resource, got %d", program.ResourceRefsEnd-program.ResourceRefsStart)
		}
		if (program.StepRefsEnd - program.StepRefsStart) != 1 {
			t.Errorf("expected 1 step, got %d", program.StepRefsEnd-program.StepRefsStart)
		}
		stepRef := ctx.StepRefs[program.StepRefsStart]
		step := ctx.StepBindingNodes[stepRef]
		if ctx.GetString(step.NameRef) != "fetch_users" {
			t.Errorf("expected step fetch_users, got %q", ctx.GetString(step.NameRef))
		}
		if (program.HandlerRefsEnd - program.HandlerRefsStart) != 2 {
			t.Errorf("expected 2 handlers, got %d", program.HandlerRefsEnd-program.HandlerRefsStart)
		}

		// Verify Workflow
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		if ctx.GetString(wf.NameRef) != "main" {
			t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
		}
		if ctx.GetString(wf.TrapRef) != "on_err" {
			t.Errorf("expected trap on_err, got %q", ctx.GetString(wf.TrapRef))
		}

		stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
		if stmtCount != 2 {
			t.Fatalf("expected 2 statements, got %d", stmtCount)
		}

		// Statement 1: fetch_users > users
		s1Ref := ctx.StatementRefs[wf.StatementRefsStart]
		s1 := ctx.PipelineStatementNodes[s1Ref]
		if ctx.GetString(s1.AssignmentRef) != "users" {
			t.Errorf("expected assignment to users, got %q", ctx.GetString(s1.AssignmentRef))
		}

		// Statement 2: users | (PRQL) | io.print
		s2Ref := ctx.StatementRefs[wf.StatementRefsStart+1]
		s2 := ctx.PipelineStatementNodes[s2Ref]
		pc := ctx.PipeChainNodes[s2.ExprRef]
		if (pc.CallRefsEnd - pc.CallRefsStart) != 3 {
			t.Errorf("expected 3 calls in pipe chain, got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}

		call1Ref := ctx.CallRefs[pc.CallRefsStart]
		call1 := ctx.CallNodes[call1Ref]
		if ctx.GetString(call1.NameRef) != "users" {
			t.Errorf("expected call users, got %q", ctx.GetString(call1.NameRef))
		}

		call2Ref := ctx.CallRefs[pc.CallRefsStart+1]
		call2 := ctx.CallNodes[call2Ref]
		if !call2.IsPrql {
			t.Errorf("expected PRQL call")
		}

		call3Ref := ctx.CallRefs[pc.CallRefsStart+2]
		call3 := ctx.CallNodes[call3Ref]
		name3 := ctx.GetString(call3.NameRef)
		if name3 == "" && call3.FunctionRef != 0 {
			fn := ctx.FunctionRefNodes[call3.FunctionRef]
			name3 = ctx.GetString(fn.ModuleRef) + "." + ctx.GetString(fn.NameRef)
		}
		if name3 != "io.print" {
			t.Errorf("expected call io.print, got %q", name3)
		}
	})
}

func TestDataframeAndAssignments(t *testing.T) {
	input := `
workflow data_test {
  [
    {
      "id": 1,
      "val": "a",
    },
    {
      "id": 2,
      "val": "b",
    },
  ]
  > my_data

  my_data
    | io.print
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		if ctx.GetString(wf.NameRef) != "data_test" {
			t.Errorf("expected workflow data_test, got %q", ctx.GetString(wf.NameRef))
		}

		stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
		if stmtCount != 2 {
			t.Fatalf("expected 2 statements, got %d", stmtCount)
		}

		// Verify dataframe assignment
		stmt1Ref := ctx.StatementRefs[wf.StatementRefsStart]
		stmt1 := ctx.PipelineStatementNodes[stmt1Ref]
		if ctx.GetString(stmt1.AssignmentRef) != "my_data" {
			t.Errorf("expected assignment to my_data, got %q", ctx.GetString(stmt1.AssignmentRef))
		}

		// Verify pipe chain
		stmt2Ref := ctx.StatementRefs[wf.StatementRefsStart+1]
		stmt2 := ctx.PipelineStatementNodes[stmt2Ref]
		pc := ctx.PipeChainNodes[stmt2.ExprRef]
		if (pc.CallRefsEnd - pc.CallRefsStart) != 2 {
			t.Errorf("expected pipe chain of length 2, got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}
	})
}

func TestFraudDetectionDAG(t *testing.T) {
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
    | produce_fraud_audits
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		// Verify Top-level counts
		if (program.ImportRefsEnd - program.ImportRefsStart) != 5 {
			t.Errorf("expected 5 imports, got %d", program.ImportRefsEnd-program.ImportRefsStart)
		}
		if (program.ResourceRefsEnd - program.ResourceRefsStart) != 3 {
			t.Errorf("expected 3 resources, got %d", program.ResourceRefsEnd-program.ResourceRefsStart)
		}
		if (program.StepRefsEnd - program.StepRefsStart) != 6 {
			t.Errorf("expected 6 steps, got %d", program.StepRefsEnd-program.StepRefsStart)
		}
		expectedStepNames := []string{
			"fetch_user_data", "fetch_risk_profile", "generate_audit",
			"fetch_transactions", "produce_fraud_audits", "produce_dead_letter_queue",
		}
		for i, name := range expectedStepNames {
			ref := ctx.StepRefs[program.StepRefsStart+uint32(i)]
			step := ctx.StepBindingNodes[ref]
			if ctx.GetString(step.NameRef) != name {
				t.Errorf("expected step %d to be %q, got %q", i, name, ctx.GetString(step.NameRef))
			}
		}

		if (program.HandlerRefsEnd - program.HandlerRefsStart) != 2 {
			t.Errorf("expected 2 handlers, got %d", program.HandlerRefsEnd-program.HandlerRefsStart)
		}

		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		if ctx.GetString(wf.NameRef) != "FraudDetection" {
			t.Errorf("expected workflow FraudDetection, got %q", ctx.GetString(wf.NameRef))
		}
		if ctx.GetString(wf.TrapRef) != "alert_on_fail" {
			t.Errorf("expected trap alert_on_fail, got %q", ctx.GetString(wf.TrapRef))
		}
	})
}

func TestAnonymousStepCall(t *testing.T) {
	input := `
import "db/pg" pg
import "std/io" io

resource db_res = pg.connection {
  host: "localhost"
}

workflow anon_test {
  data
    | <connection=db_res> pg.query {
        query: "SELECT 1"
    }
    | io.print
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		stmtRef := ctx.StatementRefs[wf.StatementRefsStart]
		stmt := ctx.PipelineStatementNodes[stmtRef]
		pc := ctx.PipeChainNodes[stmt.ExprRef]

		if (pc.CallRefsEnd - pc.CallRefsStart) != 3 {
			t.Errorf("expected 3 calls, got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}

		// Check the second call (anonymous)
		callRef := ctx.CallRefs[pc.CallRefsStart+1]
		call := ctx.CallNodes[callRef]
		if call.FunctionRef == 0 {
			t.Fatalf("expected FunctionRef for anonymous call")
		}
		fn := ctx.FunctionRefNodes[call.FunctionRef]
		if ctx.GetString(fn.ModuleRef) != "pg" || ctx.GetString(fn.NameRef) != "query" {
			t.Errorf("expected pg.query, got %s.%s", ctx.GetString(fn.ModuleRef), ctx.GetString(fn.NameRef))
		}
		if fn.ResourcesRefRef == 0 {
			t.Errorf("expected resource reference")
		}
		if fn.ConfigRef == 0 {
			t.Errorf("expected config dictionary")
		}
	})
}

func TestDataframeInPipeline(t *testing.T) {
	input := `
import "std/io" io

workflow main {
  []
    | io.print
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]

		stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
		if stmtCount != 1 {
			t.Fatalf("expected 1 statement, got %d", stmtCount)
		}

		stmtRef := ctx.StatementRefs[wf.StatementRefsStart]
		stmt := ctx.PipelineStatementNodes[stmtRef]
		pc := ctx.PipeChainNodes[stmt.ExprRef]

		if (pc.CallRefsEnd - pc.CallRefsStart) != 2 {
			t.Errorf("expected 2 calls, got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}

		// Check the first call (dataframe)
		call1Ref := ctx.CallRefs[pc.CallRefsStart]
		call1 := ctx.CallNodes[call1Ref]
		if call1.DataframeRef == 0 {
			t.Errorf("expected DataframeRef for first call")
		}

		// Check the second call (io.print)
		call2Ref := ctx.CallRefs[pc.CallRefsStart+1]
		call2 := ctx.CallNodes[call2Ref]
		name2 := ctx.GetString(call2.NameRef)
		if name2 == "" && call2.FunctionRef != 0 {
			fn := ctx.FunctionRefNodes[call2.FunctionRef]
			name2 = ctx.GetString(fn.ModuleRef) + "." + ctx.GetString(fn.NameRef)
		}
		if name2 != "io.print" {
			t.Errorf("expected call io.print, got %q", name2)
		}
	})
}

func TestComplexPipelineWithLocalTrapsAndJoins(t *testing.T) {
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
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		// Verify Top-level counts
		if (program.ImportRefsEnd - program.ImportRefsStart) != 1 {
			t.Errorf("expected 1 import, got %d", program.ImportRefsEnd-program.ImportRefsStart)
		}
		if (program.StepRefsEnd - program.StepRefsStart) != 5 {
			t.Errorf("expected 5 steps, got %d", program.StepRefsEnd-program.StepRefsStart)
		}
		expectedStepNames := []string{"s1", "s2", "s3", "s4", "r1"}
		for i, name := range expectedStepNames {
			ref := ctx.StepRefs[program.StepRefsStart+uint32(i)]
			step := ctx.StepBindingNodes[ref]
			if ctx.GetString(step.NameRef) != name {
				t.Errorf("expected step %d to be %q, got %q", i, name, ctx.GetString(step.NameRef))
			}
		}

		if (program.HandlerRefsEnd - program.HandlerRefsStart) != 2 {
			t.Errorf("expected 2 handlers, got %d", program.HandlerRefsEnd-program.HandlerRefsStart)
		}

		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		if ctx.GetString(wf.NameRef) != "main" {
			t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
		}
		if ctx.GetString(wf.TrapRef) != "recover" {
			t.Errorf("expected trap recover, got %q", ctx.GetString(wf.TrapRef))
		}

		stmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
		if stmtCount != 4 {
			t.Fatalf("expected 4 statements, got %d", stmtCount)
		}

		// Statement 1: s1 ? recover_local | s2 > pipe_assignment
		stmt1Ref := ctx.StatementRefs[wf.StatementRefsStart]
		stmt1 := ctx.PipelineStatementNodes[stmt1Ref]
		if ctx.GetString(stmt1.AssignmentRef) != "pipe_assignment" {
			t.Errorf("expected assignment to pipe_assignment, got %q", ctx.GetString(stmt1.AssignmentRef))
		}
		pc1 := ctx.PipeChainNodes[stmt1.ExprRef]
		if (pc1.CallRefsEnd - pc1.CallRefsStart) != 2 {
			t.Errorf("expected 2 calls, got %d", pc1.CallRefsEnd-pc1.CallRefsStart)
		}
		call1 := ctx.CallNodes[ctx.CallRefs[pc1.CallRefsStart]]
		if ctx.GetString(call1.NameRef) != "s1" {
			t.Errorf("expected call s1, got %q", ctx.GetString(call1.NameRef))
		}
		if ctx.GetString(call1.TrapRef) != "recover_local" {
			t.Errorf("expected trap recover_local on s1, got %q", ctx.GetString(call1.TrapRef))
		}
		call1_2 := ctx.CallNodes[ctx.CallRefs[pc1.CallRefsStart+1]]
		if ctx.GetString(call1_2.NameRef) != "s2" {
			t.Errorf("expected call s2, got %q", ctx.GetString(call1_2.NameRef))
		}

		// Statement 2: pipe_assignment | s1 | s2 > pipe_assignment_2
		stmt2Ref := ctx.StatementRefs[wf.StatementRefsStart+1]
		stmt2 := ctx.PipelineStatementNodes[stmt2Ref]
		if ctx.GetString(stmt2.AssignmentRef) != "pipe_assignment_2" {
			t.Errorf("expected assignment to pipe_assignment_2, got %q", ctx.GetString(stmt2.AssignmentRef))
		}
		pc2 := ctx.PipeChainNodes[stmt2.ExprRef]
		if (pc2.CallRefsEnd - pc2.CallRefsStart) != 3 {
			t.Errorf("expected 3 calls in statement 2, got %d", pc2.CallRefsEnd-pc2.CallRefsStart)
		}
		call2_1 := ctx.CallNodes[ctx.CallRefs[pc2.CallRefsStart]]
		if ctx.GetString(call2_1.NameRef) != "pipe_assignment" {
			t.Errorf("expected call pipe_assignment, got %q", ctx.GetString(call2_1.NameRef))
		}
		call2_2 := ctx.CallNodes[ctx.CallRefs[pc2.CallRefsStart+1]]
		if ctx.GetString(call2_2.NameRef) != "s1" {
			t.Errorf("expected call s1, got %q", ctx.GetString(call2_2.NameRef))
		}
		call2_3 := ctx.CallNodes[ctx.CallRefs[pc2.CallRefsStart+2]]
		if ctx.GetString(call2_3.NameRef) != "s2" {
			t.Errorf("expected call s2, got %q", ctx.GetString(call2_3.NameRef))
		}

		// Statement 3: (PRQL) | s3
		stmt3Ref := ctx.StatementRefs[wf.StatementRefsStart+2]
		stmt3 := ctx.PipelineStatementNodes[stmt3Ref]
		pc3 := ctx.PipeChainNodes[stmt3.ExprRef]
		call3_1 := ctx.CallNodes[ctx.CallRefs[pc3.CallRefsStart]]
		if !call3_1.IsPrql {
			t.Errorf("expected PRQL call in statement 3")
		}
		call3_2 := ctx.CallNodes[ctx.CallRefs[pc3.CallRefsStart+1]]
		if ctx.GetString(call3_2.NameRef) != "s3" {
			t.Errorf("expected call s3, got %q", ctx.GetString(call3_2.NameRef))
		}

		// Statement 4: s4 | (PRQL) | r1
		stmt4Ref := ctx.StatementRefs[wf.StatementRefsStart+3]
		stmt4 := ctx.PipelineStatementNodes[stmt4Ref]
		pc4 := ctx.PipeChainNodes[stmt4.ExprRef]
		if (pc4.CallRefsEnd - pc4.CallRefsStart) != 3 {
			t.Errorf("expected 3 calls in statement 4, got %d", pc4.CallRefsEnd-pc4.CallRefsStart)
		}
		call4_1 := ctx.CallNodes[ctx.CallRefs[pc4.CallRefsStart]]
		if ctx.GetString(call4_1.NameRef) != "s4" {
			t.Errorf("expected call s4, got %q", ctx.GetString(call4_1.NameRef))
		}
		call4_2 := ctx.CallNodes[ctx.CallRefs[pc4.CallRefsStart+1]]
		if !call4_2.IsPrql {
			t.Errorf("expected PRQL call in statement 4")
		}
		call4_3 := ctx.CallNodes[ctx.CallRefs[pc4.CallRefsStart+2]]
		if ctx.GetString(call4_3.NameRef) != "r1" {
			t.Errorf("expected call r1 in statement 4, got %q", ctx.GetString(call4_3.NameRef))
		}
	})
}

func TestWorkflowWithHandlerAndPRQL(t *testing.T) {
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
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		// Verify Import
		if (program.ImportRefsEnd - program.ImportRefsStart) != 1 {
			t.Errorf("expected 1 import, got %d", program.ImportRefsEnd-program.ImportRefsStart)
		}

		// Verify Handler
		if (program.HandlerRefsEnd - program.HandlerRefsStart) != 1 {
			t.Errorf("expected 1 handler, got %d", program.HandlerRefsEnd-program.HandlerRefsStart)
		}
		handlerRef := ctx.HandlerRefs[program.HandlerRefsStart]
		handler := ctx.HandlerNodes[handlerRef]
		if ctx.GetString(handler.NameRef) != "on_error" {
			t.Errorf("expected handler on_error, got %q", ctx.GetString(handler.NameRef))
		}

		handlerStmtCount := handler.HandlerStatementRefsEnd - handler.HandlerStatementRefsStart
		if handlerStmtCount != 1 {
			t.Fatalf("expected 1 handler statement, got %d", handlerStmtCount)
		}
		hStmtRef := ctx.HandlerStatementRefs[handler.HandlerStatementRefsStart]
		hStmt := ctx.HandlerStatementNodes[hStmtRef]
		if !hStmt.IsCatchAll {
			t.Errorf("expected catch-all (*) in handler")
		}
		// In handler, * | io.stderr results in a pipe chain with an implicit empty call first
		ps := ctx.PipelineStatementNodes[hStmt.StmtRef]
		pc := ctx.PipeChainNodes[ps.ExprRef]
		if (pc.CallRefsEnd - pc.CallRefsStart) != 2 {
			t.Errorf("expected 2 calls in handler pipe chain (implicit + stderr), got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}

		// Verify Workflow
		if (program.WorkflowRefsEnd - program.WorkflowRefsStart) != 1 {
			t.Errorf("expected 1 workflow, got %d", program.WorkflowRefsEnd-program.WorkflowRefsStart)
		}
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		if ctx.GetString(wf.NameRef) != "main" {
			t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
		}
		if ctx.GetString(wf.TrapRef) != "on_error" {
			t.Errorf("expected trap on_error, got %q", ctx.GetString(wf.TrapRef))
		}

		wfStmtCount := wf.StatementRefsEnd - wf.StatementRefsStart
		if wfStmtCount != 1 {
			t.Fatalf("expected 1 workflow statement, got %d", wfStmtCount)
		}
		wfStmtRef := ctx.StatementRefs[wf.StatementRefsStart]
		wfStmt := ctx.PipelineStatementNodes[wfStmtRef]
		wfPc := ctx.PipeChainNodes[wfStmt.ExprRef]
		if (wfPc.CallRefsEnd - wfPc.CallRefsStart) != 1 {
			t.Errorf("expected 1 call in workflow pipe chain, got %d", wfPc.CallRefsEnd-wfPc.CallRefsStart)
		}
		callRef := ctx.CallRefs[wfPc.CallRefsStart]
		call := ctx.CallNodes[callRef]
		if !call.IsPrql {
			t.Errorf("expected PRQL call")
		}
		if ctx.GetString(call.QueryRef) != "(from input)" {
			t.Errorf("expected PRQL query '(from input)', got %q", ctx.GetString(call.QueryRef))
		}
	})
}

func TestInvalidSyntax(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedErrs int
	}{
		{
			name: "invalid splat in workflow",
			input: `
import "std/io" io

workflow main {
  *
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid handler trap",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

handler error2 ? error {
  *
    | io.print
}

workflow main {
  fetch_users
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid handler trap and dataframe pipe",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

handler error2 {
  *
    | io.print
}

workflow main ? error {
  fetch_users ? error2
    | [{}]
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid dataframe pipe",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

handler error2 {
  *
    | io.print
}

workflow main ? error {
  fetch_users
    | [{}]
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "trap followed by same-line pipe",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

handler error2 {
  *
    | io.print
}

workflow main ? error {
  fetch_users ? error2 | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "asterisk followed by same-line pipe",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

handler error2 {
  * | io.print
}

workflow main ? error {
  fetch_users ? error2
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "pipe followed by pipe on same line",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

workflow main ? error {
  fetch_users | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "same-line pipe after identifier",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

workflow main ? error {
  fetch_users | (select id)
  | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "multiple pipes on same line",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

workflow main ? error {
  fetch_users | (select id) | io.print
}
`,
			expectedErrs: 2,
		},
		{
			name: "same-line pipe after trap",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

workflow main ? error {
  fetch_users ? error | (select id) | io.print
}
`,
			expectedErrs: 2,
		},
		{
			name: "same-line pipe after prql block",
			input: `
import "std/io" io

handler error {
  *
    | io.print
}

workflow main ? error {
  (select id) | io.print ? error
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid data_literal object only",
			input: `
workflow main {
  {a: 123}
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid data_literal empty object",
			input: `
workflow main {
  {}
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid data_literal nested list",
			input: `
workflow main {
  [{a: []}]
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid dataframe indentation",
			input: `
workflow main {
  [
{a: 1}
  ]
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid indentation case 1",
			input: `
workflow main {
  [
    {a: 1}]
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid indentation case 2",
			input: `
workflow main {
  [
    {a: 1
    }
  ]
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid indentation case 3",
			input: `
workflow main {
  [{a: 1
    }
  ]
}
`,
			expectedErrs: 5,
		},
		{
			name: "invalid indentation case 4",
			input: `
workflow main {
  [{a: 1}
  ]
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid step call empty dict no space",
			input: `
workflow main {
  []
    | io.test {}
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid multiline dict misaligned closer",
			input: `
workflow main {
  []
    | io.test {
        config: "test"
}
    | step_test
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid dict started same line ended new line",
			input: `
workflow main {
  []
    | io.test { config: "test"
      }
    | step_test
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid dict started same line ended new line 2",
			input: `
workflow main {
  []
    | io.test { config: "test"
}
    | step_test
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid pipe and resource ref spacing",
			input: `
workflow main {
  []
    |<test=test>io.test { config: "test" config: "test2" }
    | step_test
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid resource ref spacing",
			input: `
workflow main {
  []
    | <test=test> io.test {config: "test" config: "test2"}
    | step_test
}
`,
			expectedErrs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runParserTest(t, tt.input, tt.expectedErrs, nil)
		})
	}
}

func TestDataLiteralSyntax(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name: "empty list",
			input: `
workflow main {
  []
}
`,
		},
		{
			name: "object with unquoted key",
			input: `
workflow main {
  [{a: 123}]
}
`,
		},
		{
			name: "object with unquoted key and trailing comma",
			input: `
workflow main {
  [{a: 123},]
}
`,
		},
		{
			name: "object with quoted key",
			input: `
workflow main {
  [{"a": 123}]
}
`,
		},
		{
			name: "object with quoted key and trailing comma",
			input: `
workflow main {
  [{"a": 123},]
}
`,
		},
		{
			name: "multiple objects with trailing comma",
			input: `
workflow main {
  [{"a": "text"},{"b": "text"},]
}
`,
		},
		{
			name: "multiline indentation organization 1",
			input: `
workflow main {
  [{
    "a": "a"
  }]
}
`,
		},
		{
			name: "multiline indentation organization 2",
			input: `
workflow main {
  [
    {
      "a": "a"
    }
  ]
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runParserTest(t, tt.input, 0, nil)
		})
	}
}

func TestFlexibleSyntax(t *testing.T) {
	input := `
import "test/test" test

step test = test.test { config: "..."}

handler test1 {}

handler test2 {
}

handler test3 {

}

workflow data_test {
  [{ "id": 1, "val": "a"}, { "id": 2, "val": "b"}]
    | test ? test1
    | test.test { config: "..."}
    | test.test { config: "..."} ? test1
  > my_data

  my_data
    | io.print
}

workflow data_test2 {}

workflow data_test3 {

}

workflow data_test4 {
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]

		// Check data_test workflow (first workflow)
		stmtRef := ctx.StatementRefs[wf.StatementRefsStart]
		stmt := ctx.PipelineStatementNodes[stmtRef]
		pc := ctx.PipeChainNodes[stmt.ExprRef]

		// Call 1: dataframe
		// Call 2: test.test { config: "..." }
		// Call 3: test.test { config: "..." } ? test1

		if (pc.CallRefsEnd - pc.CallRefsStart) != 4 {
			t.Errorf("expected 4 calls in data_test statement 1, got %d", pc.CallRefsEnd-pc.CallRefsStart)
		}

		call4Ref := ctx.CallRefs[pc.CallRefsStart+3]
		call4 := ctx.CallNodes[call4Ref]

		if call4.FunctionRef == 0 {
			t.Errorf("expected FunctionRef on call 3")
		}
		if ctx.GetString(call4.TrapRef) != "test1" {
			t.Errorf("expected trap test1 on call 3, got %q", ctx.GetString(call4.TrapRef))
		}
	})
}

func TestLiteralsAndLists(t *testing.T) {
	input := `
workflow lit_test {
  [
    {
      "string": "hello",
      "int": 42,
      "float": 3.14,
      "bool_true": true,
      "bool_false": false,
      "null_val": null
    }
  ]
  > data
}
`
	runParserTest(t, input, 0, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
		wf := ctx.WorkflowNodes[wfRef]
		stmtRef := ctx.StatementRefs[wf.StatementRefsStart]
		stmt := ctx.PipelineStatementNodes[stmtRef]
		df := ctx.DataframeNodes[stmt.ExprRef]

		if (df.DictRefsEnd - df.DictRefsStart) != 1 {
			t.Errorf("expected 1 dict in dataframe, got %d", df.DictRefsEnd-df.DictRefsStart)
		}

		dictRef := ctx.DictRefs[df.DictRefsStart]
		dict := ctx.DictNodes[dictRef]
		pairCount := dict.PairRefsEnd - dict.PairRefsStart
		if pairCount != 6 {
			t.Errorf("expected 6 pairs in dict, got %d", pairCount)
		}
	})
}

func TestTopLevelSynchronization(t *testing.T) {
	input := `
unexpected_token
import "std/io" io

workflow main {
  []
    | io.print
}
`
	// Expect 1 error for "unexpected_token"
	runParserTest(t, input, 1, func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
		// Should still have successfully parsed the import and workflow after synchronization
		if (program.ImportRefsEnd - program.ImportRefsStart) != 1 {
			t.Errorf("expected 1 import, got %d", program.ImportRefsEnd-program.ImportRefsStart)
		}
		if (program.WorkflowRefsEnd - program.WorkflowRefsStart) != 1 {
			t.Errorf("expected 1 workflow, got %d", program.WorkflowRefsEnd-program.WorkflowRefsStart)
		}
	})
}

func TestStepCallSyntax(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedErrs int
	}{
		{
			name: "valid empty dict with space",
			input: `
workflow main {
  []
    | io.test { }
}
`,
		},
		{
			name: "valid dict one line",
			input: `
workflow main {
  []
    | io.test { config: "test" }
}
`,
		},
		{
			name: "valid dict one line multiple pairs",
			input: `
workflow main {
  []
    | io.test { config: "test", config2: "test2" }
}
`,
		},
		{
			name: "valid multiline dict",
			input: `
workflow main {
  []
    | io.test {
        config: "test"
        config: "test2"
    }
    | step_test
}
`,
		},
		{
			name: "valid multiline dict with resource ref",
			input: `
workflow main {
  []
    | <test=test> io.test {
        config: "test"
        config: "test2"
    }
    | step_test
}
`,
		},
		{
			name: "valid one line dict with resource ref",
			input: `
workflow main {
  []
    | <test=test> io.test { config: "test", config2: "test2" }
}
`,
		},
		{
			name: "invalid indentation case 1",
			input: `
workflow main {
  [
    {a: 1}]
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid indentation case 2",
			input: `
workflow main {
  [
    {a: 1
    }
  ]
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid indentation case 3",
			input: `
workflow main {
  [{a: 1
    }
  ]
}
`,
			expectedErrs: 5,
		},
		{
			name: "invalid indentation case 4",
			input: `
workflow main {
  [{a: 1}
  ]
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid step call empty dict no space",
			input: `
workflow main {
  []
    | io.test {}
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid multiline dict misaligned closer",
			input: `
workflow main {
  []
    | io.test {
        config: "test"
}
    | step_test
}
`,
			expectedErrs: 1,
		},
		{
			name: "invalid dict started same line ended new line",
			input: `
workflow main {
  []
    | io.test { config: "test"
      }
    | step_test
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid dict started same line ended new line 2",
			input: `
workflow main {
  []
    | io.test { config: "test"
}
    | step_test
}
`,
			expectedErrs: 3,
		},
		{
			name: "invalid pipe and resource ref spacing",
			input: `
workflow main {
  []
    |<test=test>io.test { config: "test" config: "test2" }
    | step_test
}
`,
			expectedErrs: 2,
		},
		{
			name: "invalid resource ref spacing",
			input: `
workflow main {
  []
    | <test=test> io.test {config: "test" config: "test2"}
    | step_test
}
`,
			expectedErrs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)
			p := New(l, ctx)
			p.Parse()
			errs := p.Errors()
			if len(errs) != tt.expectedErrs {
				t.Errorf("expected %d errors, got %d: %v", tt.expectedErrs, len(errs), errs)
			}
		})
	}
}

func TestWorkflowBlankLineSeparation(t *testing.T) {
	input := `
workflow hello_world ? error_print {
  []
    | io.print

  <broker=kf_broker> kafka.consume {
    topic: "live_transactions"
  }
  > tx_stream
}
`
	l := lexer.New(input)
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)
	p := New(l, ctx)
	p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("expected no errors, got %v", p.Errors())
	}

}

func TestResourceRefNewline(t *testing.T) {
	input := `
workflow res_newline {
  <connection=db>
  pg.query { query: "SELECT 1" }
}
`
	runParserTest(t, input, 0, nil)
}

func TestUserSpecificExample(t *testing.T) {
	input := `
workflow FraudDetection ? alert_on_fail {
  <broker=kf_broker> kafka.consume {
    topic: "live_transactions"
  }
  > tx_stream

  tx_stream
    | draft_final_audit
    | <broker=kf_broker> kafka.produce {
        topic: "fraud_audits"
      }
}
`
	l := lexer.New(input)
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)
	p := New(l, ctx)
	p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("expected no errors, got %v", p.Errors())
	}

}
