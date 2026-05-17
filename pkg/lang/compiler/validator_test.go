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

	v := NewValidator(prog, ctx, nil)
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

	v := NewValidator(prog, ctx, nil)
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

	v := NewValidator(prog, ctx, nil)
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

	v := NewValidator(prog, ctx, nil)
	err := v.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "undefined handler 'missing_handler'")
}

func TestValidator_UnusedStepRange(t *testing.T) {
	input := `step unused_step = openai.prompt {
  system: "You are a specialized agent."
}

step used_step = openai.prompt {
  system: "I am used."
}

workflow main {
  used_step
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var unusedDiag *ValidationError
	for _, err := range errs {
		if err.Message == "unused step: unused_step" {
			unusedDiag = &err
			break
		}
	}

	require.NotNil(t, unusedDiag)
	assert.Equal(t, uint32(1), unusedDiag.Range.Start.Line)
	assert.Equal(t, uint32(3), unusedDiag.Range.End.Line)
}

func TestValidator_DuplicateImportAlias(t *testing.T) {
	input := `
import "std/io" io
import "other/io" io

workflow main {
  io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "duplicate import alias: io" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected duplicate import alias error")
}

func TestValidator_DuplicateStep(t *testing.T) {
	input := `
step my_step = openai.prompt { system: "A" }
step my_step = openai.prompt { system: "B" }

workflow main {
  my_step
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "duplicate step definition: my_step" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected duplicate step definition error")
}

func TestValidator_DuplicateResource(t *testing.T) {
	input := `
resource db = pg.connection { host: "A" }
resource db = pg.connection { host: "B" }

workflow main {
  []
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "duplicate resource definition: db" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected duplicate resource definition error")
}

func TestValidator_DuplicateWorkflow(t *testing.T) {
	input := `
workflow main {
  []
}

workflow main {
  []
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "duplicate workflow definition: main" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected duplicate workflow definition error")
}

func TestValidator_ConflictImportWithStep(t *testing.T) {
	input := `
import "std/io" my_name
step my_name = openai.prompt { system: "A" }

workflow main {
  []
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "name 'my_name' conflicts with an import alias" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected import conflict with step error")
}

func TestValidator_ConflictStepWithResource(t *testing.T) {
	input := `
step my_name = openai.prompt { system: "A" }
resource my_name = pg.connection { host: "localhost" }

workflow main {
  []
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "name 'my_name' conflicts with a resource name" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected step conflict with resource error")
}

func TestValidator_ConflictResourceWithWorkflow(t *testing.T) {
	input := `
resource my_name = pg.connection { host: "localhost" }
workflow my_name {
  []
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "name 'my_name' conflicts with a resource name" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected resource conflict with workflow error")
}

func TestValidator_ConflictWorkflowWithImport(t *testing.T) {
	input := `
workflow my_name {
  []
}
import "std/io" my_name
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx, nil)
	errs := v.ValidateAll()

	var found bool
	for _, err := range errs {
		if err.Message == "name 'my_name' conflicts with an import alias" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected workflow conflict with import error")
}


