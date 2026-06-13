package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
	"github.com/galgotech/heddle-lang/pkg/schema"
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
  [{"col": 1}]
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
  [{"col": 1}]
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

func TestValidator_CastAndPrintSuccess(t *testing.T) {
	input := `
import "std/col"
import "std/io"

step my_data = some_source { }

workflow main {
  my_data
    | col.cast { to: "string" }
    | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
				{Name: "name", ArrowType: "utf8"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	assert.NoError(t, err)
}

func TestValidator_CastFailure(t *testing.T) {
	input := `
import "std/col"

step my_data = some_source { }

workflow main {
  my_data
    | col.cast { to: "invalid_type" }
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cast: invalid target type")
}

func TestValidator_CastColumnsSuccess(t *testing.T) {
	input := `
import "std/col"

step my_data = some_source { }

workflow main {
  my_data
    | col.cast { columns: { id: "string" } }
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
				{Name: "name", ArrowType: "utf8"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	assert.NoError(t, err)
}

func TestValidator_CastMissingConfig(t *testing.T) {
	input := `
import "std/col"

step my_data = some_source { }

workflow main {
  my_data
    | col.cast { }
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cast: config must specify either 'columns' map or global 'to' type")
}

func TestValidator_PrintUnprintableType(t *testing.T) {
	input := `
import "std/io"

step my_data = some_source { }

workflow main {
  my_data
    | io.print
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "data", ArrowType: "struct"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "print: column 'data' has unprintable type 'struct'")
}

func TestValidator_PrintAfterPrintFailure(t *testing.T) {
	input := `
import "std/io"

step my_data = some_source { }
step other_step = some_receiver { }

workflow main {
  my_data
    | io.print
    | other_step
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	schemas := map[string]schema.StepSchemas{
		"some_source": {
			Output: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
		},
		"some_receiver": {
			Input: []schema.ColumnSchema{
				{Name: "id", ArrowType: "int64"},
			},
		},
	}

	v := NewValidator(prog, ctx, schemas)
	err := v.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema mismatch: output has 0 fields, input has 1 fields")
}

func TestValidator_PRQLJoinsAndConflicts(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{
			name: "PRQL Syntax Error",
			input: `
step step_a = some_source { }
workflow main {
  step_a > table_a
  (from table_a | filter invalid_syntax {)
}
`,
			expectedErr: "PRQL compilation error",
		},
		{
			name: "PRQL Valid Join with Existing Assignments",
			input: `
step step_a = some_source { }
step step_b = some_source { }

workflow main {
  step_a > table_a
  step_b > table_b
  (from table_a | join table_b (==col_a) | select {table_a.col_a, table_b.col_b})
}
`,
			expectedErr: "",
		},
		{
			name: "PRQL Join with Undefined Assignment",
			input: `
step step_a = some_source { }

workflow main {
  step_a > table_a
  (from table_a | join table_missing (==col_a) | select {table_a.col_a})
}
`,
			expectedErr: "undefined assignment referenced in PRQL: table_missing",
		},
		{
			name: "PRQL Alias Conflict with Heddle Assignment",
			input: `
step step_a = some_source { }
step step_b = some_source { }
step step_c = some_source { }

workflow main {
  step_a > table_a
  step_b > table_b
  step_c > table_c
  (from table_a | join table_c=table_b (==col_a))
}
`,
			expectedErr: "naming conflict: PRQL alias 'table_c' conflicts with Heddle assignment",
		},
		{
			name: "PRQL Input at start of pipeline (Error)",
			input: `
workflow main {
  (from input)
}
`,
			expectedErr: "cannot use 'input' table at the start of a pipeline; it can only be used when PRQL is piped after a '|' operator",
		},
		{
			name: "PRQL Input piped after a step (Success)",
			input: `
step step_a = some_source { }
workflow main {
  step_a
    | (from input)
}
`,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ast.AcquireASTContext()
			defer ast.ReleaseASTContext(ctx)

			l := lexer.New(tt.input)
			p := parser.New(l, ctx)
			prog := p.Parse()
			require.Empty(t, p.Errors())

			v := NewValidator(prog, ctx, nil)
			err := v.Validate()
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestValidator_LoadCSVSuccess(t *testing.T) {
	input := `
	import "std/io"

	step my_loader = io.load_csv {
		path: "my_file.csv",
		delimiter: ",",
		lazy_quotes: true,
		columns: {
			id: "int64",
			name: "string",
			active: "bool"
		}
	}

	workflow main {
		my_loader
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

func TestValidator_LoadCSVMissingColumns(t *testing.T) {
	input := `
	import "std/io"

	step my_loader = io.load_csv {
		path: "my_file.csv"
	}

	workflow main {
		my_loader
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
	assert.Contains(t, err.Error(), "load_csv: 'columns' is required in config")
}
