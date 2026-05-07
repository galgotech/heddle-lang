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

step fetch_users = pg.query <connection=res_pg> {
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
				wfCount := program.WorkflowRefsEnd - program.WorkflowRefsStart
				if wfCount != 1 {
					t.Fatalf("expected 1 workflow, got %d", wfCount)
				}
				wfRef := ctx.WorkflowRefs[program.WorkflowRefsStart]
				wf := ctx.WorkflowNodes[wfRef]
				if ctx.GetString(wf.NameRef) != "main" {
					t.Errorf("expected workflow main, got %q", ctx.GetString(wf.NameRef))
				}
				if ctx.GetString(wf.TrapRef) != "on_err" {
					t.Errorf("expected trap on_err, got %q", ctx.GetString(wf.TrapRef))
				}
			},
		},
		{
			name: "Dataframe and Assignments",
			input: `
workflow data_test {
  [
    {
      "id": 1
      "val": "a"
    },
    {
      "id": 2
      "val": "b"
    }
  ]
  > my_data

  my_data
    | io.print
}
`,
			expectedErrs: 0,
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
