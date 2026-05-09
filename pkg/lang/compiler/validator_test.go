package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestValidator_ResourceInjection(t *testing.T) {
	input := `
resource pg_db = pg.connection {
  host: "localhost"
}

step fetch_users = <connection=pg_db> pg.query {
  query: "SELECT * FROM users"
}

workflow main {
  fetch_users
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidator_UndefinedResource(t *testing.T) {
	input := `
step fetch_users = <connection=missing_db> pg.query {
  query: "SELECT * FROM users"
}

workflow main {
  fetch_users
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "undefined resource 'missing_db'")
}

func TestValidator_HandlerReference(t *testing.T) {
	input := `
handler err_log {
  *
    | io.stderr
}

workflow main ? err_log {
  (from input select *)
    | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()

	assert.NoError(t, err)
}

func TestValidator_UndefinedHandler(t *testing.T) {
	input := `
workflow main ? missing_handler {
  (from input select *)
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "undefined handler 'missing_handler'")
}
