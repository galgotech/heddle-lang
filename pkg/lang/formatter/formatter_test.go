package formatter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestFormatter_Basic(t *testing.T) {
	input := `import "pg" pg
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
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	f := New(ctx)
	formatted := f.Format(prog)

	// Note: Our formatter might change minor things like spacing/newlines
	// but the structure should be the same.
	assert.Contains(t, formatted, "import \"pg\" pg")
	assert.Contains(t, formatted, "resource db = pg.connection {")
	assert.Contains(t, formatted, "step query = <connection=db> pg.query {")
	assert.Contains(t, formatted, "workflow main {")
	assert.Contains(t, formatted, "  query\n  | io.print")
}
