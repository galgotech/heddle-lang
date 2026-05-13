package formatter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestFormatter(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)

			l := lexer.New(tt.before)
			p := parser.New(l, ctx)
			prog := p.Parse()
			require.Empty(t, p.Errors())

			f := New(ctx)
			formatted := f.Format(prog)

			assert.Equal(t, tt.after, formatted, "failed test: %s", tt.name)
		})
	}
}
