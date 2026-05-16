package lsp

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestHandleFormatting(t *testing.T) {
	ctx := context.Background()
	testURI := protocol.DocumentURI("file:///test.he")

	tests := []struct {
		name           string
		setupFiles     func(*sync.Map)
		expectedEdits  []protocol.TextEdit
		expectedErr    bool
		malformedParam bool
	}{
		{
			name: "format valid code",
			setupFiles: func(m *sync.Map) {
				m.Store(testURI, `import "std/io" io

workflow main {
  []
    | io.print
}`)
			},
			expectedEdits: []protocol.TextEdit{
				{
					Range: protocol.Range{
						Start: protocol.Position{Line: 0, Character: 0},
						End:   protocol.Position{Line: 100000, Character: 0},
					},
					NewText: `import "std/io" io

workflow main {
  []
    | io.print
}
`,
				},
			},
		},
		{
			name: "format invalid code",
			setupFiles: func(m *sync.Map) {
				m.Store(testURI, "workflow {")
			},
			expectedEdits: []protocol.TextEdit{
				{
					Range: protocol.Range{
						Start: protocol.Position{Line: 0, Character: 0},
						End:   protocol.Position{Line: 100000, Character: 0},
					},
					NewText: "workflow  {\n}\n",
				},
			},
		},
		{
			name: "file not found",
			setupFiles: func(m *sync.Map) {
				// empty
			},
			expectedEdits: nil,
		},
		{
			name: "format workflow with redirects and blank lines",
			setupFiles: func(m *sync.Map) {
				m.Store(testURI, `workflow hello_world {
  test

  > test2

  test2
    | io.print

  []
    | io.print
}`)
			},
			expectedEdits: []protocol.TextEdit{
				{
					Range: protocol.Range{
						Start: protocol.Position{Line: 0, Character: 0},
						End:   protocol.Position{Line: 100000, Character: 0},
					},
					NewText: `workflow hello_world {
  test
  > test2

  test2
    | io.print

  []
    | io.print
}
`,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := &sync.Map{}
			if tt.setupFiles != nil {
				tt.setupFiles(files)
			}

			params := protocol.DocumentFormattingParams{
				TextDocument: protocol.TextDocumentIdentifier{
					URI: testURI,
				},
			}

			var req jsonrpc2.Request
			if tt.malformedParam {
				// We can't easily create a malformed request with NewCall as it takes interface{}
			} else {
				call, _ := jsonrpc2.NewCall(jsonrpc2.NewNumberID(1), "textDocument/formatting", params)
				req = call
			}

			var capturedResult interface{}
			var capturedErr error
			replier := func(ctx context.Context, result interface{}, err error) error {
				capturedResult = result
				capturedErr = err
				return nil
			}

			err := HandleFormatting(ctx, replier, req, files)
			assert.NoError(t, err)

			if tt.expectedErr {
				assert.Error(t, capturedErr)
			} else {
				assert.NoError(t, capturedErr)
				if tt.expectedEdits == nil {
					assert.Nil(t, capturedResult)
				} else {
					assert.Equal(t, tt.expectedEdits, capturedResult)
				}
			}
		})
	}
}

func TestFormatter(t *testing.T) {
	tests := []struct {
		name         string
		before       string
		after        string
		expectErrors bool
	}{
		{
			name: "Basic",
			before: `import "pg" pg
import "io"

resource db = pg.connection {
  host: "localhost"
}

step query = <connection=db> pg.query {
  query: "SELECT 1"
}

workflow main {
  query
  | io.print
}
`,
			after: `import "pg" pg
import "io"

resource db = pg.connection {
  host: "localhost"
}

step query = <connection=db> pg.query {
  query: "SELECT 1"
}

workflow main {
  query
    | io.print
}
`,
		},
		{
			name: "PipeFirst",
			before: `import "std/io" io

handler error_print {
  *
    | io.print
}

workflow hello_world ? error_print {
  []
  | io.print
}
`,
			after: `import "std/io" io

handler error_print {
  *
    | io.print
}

workflow hello_world ? error_print {
  []
    | io.print
}
`,
		},
		{
			name: "AssignmentNewline",
			before: `workflow w {
  <broker=kf_broker> kafka.consume { topic: "live_transactions" }
  > tx_stream

  <broker=kf_broker> kafka.consume {
    topic: "live_transactions"
  }
  > tx_stream2
}
`,
			after: `workflow w {
  <broker=kf_broker> kafka.consume { topic: "live_transactions" }
  > tx_stream

  <broker=kf_broker> kafka.consume {
    topic: "live_transactions"
  }
  > tx_stream2
}
`,
		},
		{
			name: "HandlerAssignment",
			before: `handler h {
  *
    | call { a: 1 }
    > out
}
`,
			after: `handler h {
  *
    | call { a: 1 }
  > out
}
`,
		},
		{
			name: "WorkflowComplexIndentation",
			before: `workflow FraudDetection {
  tx_stream
    | draft_final_audit
    | <broker=kf_broker> kafka.produce {
      topic: "fraud_audits"
    }
}
`,
			after: `workflow FraudDetection {
  tx_stream
    | draft_final_audit
    | <broker=kf_broker> kafka.produce {
        topic: "fraud_audits"
      }
}
`,
		},
		{
			name: "HandlerIndentation",
			before: `handler alert_step_fail {
  *
    | <broker=kf_broker> kafka.produce {
      topic: "dlq_alerts"
    }
}
`,
			after: `handler alert_step_fail {
  *
    | <broker=kf_broker> kafka.produce {
        topic: "dlq_alerts"
      }
}
`,
		},
		{
			name: "PipeSameLine",
			before: `workflow main {
  [] | io.print
}
`,
			after: `workflow main {
  []
    | io.print
}
`,
			expectErrors: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)

			l := lexer.New(tt.before)
			p := parser.New(l, ctx)
			prog := p.Parse()
			if tt.expectErrors {
				assert.NotEmpty(t, p.Errors(), "expected errors for test: %s", tt.name)
			} else {
				require.Empty(t, p.Errors(), "unexpected errors for test: %s", tt.name)
			}

			f := NewFormatter(ctx)
			formatted := f.Format(prog)

			assert.Equal(t, tt.after, formatted, "failed test: %s", tt.name)
		})
	}
}
