package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

func TestParser(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedErrs int
		check        func(*testing.T, *ast.ASTContext, ast.ProgramNode)
	}{
		{
			name: "full feature workflow",
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
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				// Verify Top-level counts
				if (program.ImportRefsEnd - program.ImportRefsStart) != 2 {
					t.Errorf("expected 2 imports, got %d", program.ImportRefsEnd-program.ImportRefsStart)
				}
				if (program.ResourceRefsEnd - program.ResourceRefsStart) != 1 {
					t.Errorf("expected 1 resource, got %d", program.ResourceRefsEnd-program.ResourceRefsStart)
				}
				if (program.StepRefsEnd - program.StepRefsStart) != 1 {
					t.Errorf("expected 1 step, got %d", program.StepRefsEnd-program.StepRefsStart)
				}

				// Verify GetString on Step name
				stepRef := ctx.StepRefs[program.StepRefsStart]
				step := ctx.StepBindingNodes[stepRef]
				if ctx.GetString(step.NameRef) != "fetch_users" {
					t.Errorf("expected step name 'fetch_users', got %q", ctx.GetString(step.NameRef))
				}

				// Verify Workflow
				wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
				wf := ctx.WorkflowNodes[wfRef]
				if ctx.GetString(wf.NameRef) != "main" {
					t.Errorf("expected workflow 'main', got %q", ctx.GetString(wf.NameRef))
				}
				if ctx.GetString(wf.TrapRef) != "on_err" {
					t.Errorf("expected trap 'on_err', got %q", ctx.GetString(wf.TrapRef))
				}

				// Verify Assignments and Calls
				s1Ref := ctx.StatementRefs[wf.StatementRefsStart]
				s1 := ctx.PipelineStatementNodes[s1Ref]
				if ctx.GetString(s1.AssignmentRef) != "users" {
					t.Errorf("expected assignment to 'users', got %q", ctx.GetString(s1.AssignmentRef))
				}

				s2Ref := ctx.StatementRefs[wf.StatementRefsStart+1]
				s2 := ctx.PipelineStatementNodes[s2Ref]
				pc := ctx.PipeChainNodes[s2.ExprRef]
				call1 := ctx.CallNodes[ctx.CallRefs[pc.CallRefsStart]]
				if ctx.GetString(call1.NameRef) != "users" {
					t.Errorf("expected call 'users', got %q", ctx.GetString(call1.NameRef))
				}

				call3 := ctx.CallNodes[ctx.CallRefs[pc.CallRefsStart+2]]
				name3 := ctx.GetString(call3.NameRef)
				if name3 == "" && call3.FunctionRef != 0 {
					fn := ctx.FunctionRefNodes[call3.FunctionRef]
					name3 = ctx.GetString(fn.ModuleRef) + "." + ctx.GetString(fn.NameRef)
				}
				if name3 != "io.print" {
					t.Errorf("expected call 'io.print', got %q", name3)
				}
			},
		},
		{
			name: "dataframe and assignments",
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
					t.Errorf("expected workflow 'data_test', got %q", ctx.GetString(wf.NameRef))
				}

				stmt1 := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				if ctx.GetString(stmt1.AssignmentRef) != "my_data" {
					t.Errorf("expected assignment to 'my_data', got %q", ctx.GetString(stmt1.AssignmentRef))
				}
			},
		},
		{
			name: "fraud detection DAG",
			input: `
import "fhub/kafka" kafka
import "fhub/postgresql" pg
import "fhub/clickhouse" ch
import "fhub/llm" openai
import "fraud-score/detect" fraud_detection

resource pg_db = pg.connection { host: "pg.internal" } 
resource ch_db = ch.connection { host: "ch.internal" }
resource kf_broker = kafka.connection { broker: "kafka.internal:9092" }

step fetch_user_data = <connection=pg_db> pg.query {
  query: "SELECT id AS user_id, country FROM users WHERE id = @user_id"
}

handler alert_on_fail {
  *
    | produce_dead_letter_queue
}

workflow FraudDetection ? alert_on_fail {
  fetch_transactions
  > tx_stream

  tx_stream
    | (from input filter amount > 10000 select {user_id, amount}) 
    | fraud_detection.process
    | produce_fraud_audits
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				if (program.ImportRefsEnd - program.ImportRefsStart) != 5 {
					t.Errorf("expected 5 imports, got %d", program.ImportRefsEnd-program.ImportRefsStart)
				}
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				if ctx.GetString(wf.NameRef) != "FraudDetection" {
					t.Errorf("expected workflow 'FraudDetection', got %q", ctx.GetString(wf.NameRef))
				}
				if ctx.GetString(wf.TrapRef) != "alert_on_fail" {
					t.Errorf("expected trap 'alert_on_fail', got %q", ctx.GetString(wf.TrapRef))
				}
			},
		},
		{
			name: "anonymous step call",
			input: `
import "db/pg" pg

workflow anon_test {
  data
    | <connection=db_res> pg.query { query: "SELECT 1" }
    | io.print
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				stmt := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				pc := ctx.PipeChainNodes[stmt.ExprRef]
				call := ctx.CallNodes[ctx.CallRefs[pc.CallRefsStart+1]]
				fn := ctx.FunctionRefNodes[call.FunctionRef]
				if ctx.GetString(fn.ModuleRef) != "pg" || ctx.GetString(fn.NameRef) != "query" {
					t.Errorf("expected 'pg.query', got %s.%s", ctx.GetString(fn.ModuleRef), ctx.GetString(fn.NameRef))
				}
			},
		},
		{
			name: "complex pipeline with local traps and joins",
			input: `
import "std/m" m

step s1 = m.extract

handler recover {
  *
    | r1
}

workflow main ? recover {
  s1 ? recover_local
    | s2
  > pipe_assignment
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				stmt := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				pc := ctx.PipeChainNodes[stmt.ExprRef]
				call := ctx.CallNodes[ctx.CallRefs[pc.CallRefsStart]]
				if ctx.GetString(call.NameRef) != "s1" || ctx.GetString(call.TrapRef) != "recover_local" {
					t.Errorf("expected s1 with trap recover_local, got %s with %s", ctx.GetString(call.NameRef), ctx.GetString(call.TrapRef))
				}
			},
		},
		{
			name: "workflow with handler and PRQL",
			input: `
import "std/io" io

handler on_error {
  *
    | io.stderr
}

workflow main ? on_error {
  (from input)
}
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				if ctx.GetString(wf.TrapRef) != "on_error" {
					t.Errorf("expected trap 'on_error', got %q", ctx.GetString(wf.TrapRef))
				}
				stmt := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				pc := ctx.PipeChainNodes[stmt.ExprRef]
				call := ctx.CallNodes[ctx.CallRefs[pc.CallRefsStart]]
				if !call.IsPrql || ctx.GetString(call.QueryRef) != "(from input)" {
					t.Errorf("expected PRQL '(from input)', got %q", ctx.GetString(call.QueryRef))
				}
			},
		},
		// Invalid Syntax Cases
		{
			name: "invalid splat in workflow",
			input: `
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
handler error2 ? error {
  *
    | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "trap followed by same-line pipe",
			input: `
workflow main ? error {
  fetch_users ? error2 | io.print
}
`,
			expectedErrs: 1,
		},
		{
			name: "multiple pipes on same line",
			input: `
workflow main {
  fetch_users | (select id) | io.print
}
`,
			expectedErrs: 2,
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
			name: "resource ref with newline",
			input: `
workflow res_newline {
  <connection=db>
  pg.query { query: "SELECT 1" }
}
`,
			expectedErrs: 1,
		},
		// Top-level Synchronization
		{
			name: "top-level synchronization after error",
			input: `
unexpected_token
import "std/io" io
workflow main {
  []
    | io.print
}
`,
			expectedErrs: 1,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				if (program.ImportRefsEnd - program.ImportRefsStart) != 1 {
					t.Errorf("expected 1 import, got %d", program.ImportRefsEnd-program.ImportRefsStart)
				}
				if (program.WorkflowRefsEnd - program.WorkflowRefsStart) != 1 {
					t.Errorf("expected 1 workflow, got %d", program.WorkflowRefsEnd-program.WorkflowRefsStart)
				}
			},
		},
		// Flexible Syntax and Data Literals
		{
			name:         "empty list literal",
			input:        `workflow main { [] }`,
			expectedErrs: 0,
		},
		{
			name:         "object with unquoted key",
			input:        `workflow main { [{a: 123}] }`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				stmt := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				df := ctx.DataframeNodes[stmt.ExprRef]
				dict := ctx.DictNodes[ctx.DictRefs[df.DictRefsStart]]
				pair := ctx.PairNodes[ctx.PairRefs[dict.PairRefsStart]]
				if ctx.GetString(pair.KeyRef) != "a" {
					t.Errorf("expected key 'a', got %q", ctx.GetString(pair.KeyRef))
				}
			},
		},
		{
			name: "flexible syntax with multiple workflows",
			input: `
workflow data_test2 {}
workflow data_test3 { }
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				if (program.WorkflowRefsEnd - program.WorkflowRefsStart) != 2 {
					t.Errorf("expected 2 workflows, got %d", program.WorkflowRefsEnd-program.WorkflowRefsStart)
				}
			},
		},
		{
			name: "literals and lists",
			input: `
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
`,
			expectedErrs: 0,
			check: func(t *testing.T, ctx *ast.ASTContext, program ast.ProgramNode) {
				wf := ctx.WorkflowNodes[ctx.WorkflowRefs[program.WorkflowRefsStart]]
				stmt := ctx.PipelineStatementNodes[ctx.StatementRefs[wf.StatementRefsStart]]
				df := ctx.DataframeNodes[stmt.ExprRef]
				dict := ctx.DictNodes[ctx.DictRefs[df.DictRefsStart]]
				if (dict.PairRefsEnd - dict.PairRefsStart) != 6 {
					t.Errorf("expected 6 pairs, got %d", dict.PairRefsEnd-dict.PairRefsStart)
				}
			},
		},
		{
			name: "workflow blank line separation",
			input: `
workflow hello_world ? error_print {
  []
    | io.print

  <broker=kf_broker> kafka.consume {
    topic: "live_transactions"
  }
  > tx_stream
}
`,
			expectedErrs: 0,
		},
		{
			name: "user specific example",
			input: `
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
`,
			expectedErrs: 0,
		},
		{
			name: "step call syntax - valid multiline dict",
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
			expectedErrs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runParserTest(t, tt.input, tt.expectedErrs, tt.check)
		})
	}
}

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
