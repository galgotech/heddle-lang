package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

func TestParserV010(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedErrs int
		check        func(*testing.T, *ast.ASTContext, ast.ProgramNode)
	}{
		{
			name: "Full Feature Workflow",
			input: `
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

workflow main ? on_err {
  fetch_users
  > users

  users
    | (from input select {id, email})
    | io.print
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
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
				if (program.HandlerRefsEnd - program.HandlerRefsStart) != 1 {
					t.Errorf("expected 1 handler, got %d", program.HandlerRefsEnd-program.HandlerRefsStart)
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
				if ctx.GetString(call3.NameRef) != "io.print" {
					t.Errorf("expected call io.print, got %q", ctx.GetString(call3.NameRef))
				}
			},
		},
		{
			name: "Dataframe and Assignments",
			input: `
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
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
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
			},
		},
		{
			name: "Fraud Detection DAG",
			input: `
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
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)

			p := New(l, ctx)
			program := p.Parse()

			errs := p.Errors()
			if len(errs) != tt.expectedErrs {
				t.Fatalf("expected %d errors, got %d: %v", tt.expectedErrs, len(errs), errs)
			}

			if tt.check != nil {
				tt.check(t, ctx, program)
			}
		})
	}
}
