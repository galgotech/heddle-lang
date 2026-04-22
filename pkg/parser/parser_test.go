package parser

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/lexer"
)

func TestImportStatements(t *testing.T) {
	input := `
import "std/http" http
import "lib/utils" utils
`
	l := lexer.New(input)
	p := New(l)
	program := p.Parse()
	checkParserErrors(t, p)

	if len(program.Statements) != 2 {
		t.Fatalf("program.Statements does not contain 2 statements. got=%d", len(program.Statements))
	}

	tests := []struct {
		expectedPath  string
		expectedAlias string
	}{
		{"\"std/http\"", "http"},
		{"\"lib/utils\"", "utils"},
	}

	for i, tt := range tests {
		stmt := program.Statements[i]
		if !testImportStatement(t, stmt, tt.expectedPath, tt.expectedAlias) {
			return
		}
	}
}

func testImportStatement(t *testing.T, s ast.Statement, path string, alias string) bool {
	if s.TokenLiteral() != "import" {
		t.Errorf("s.TokenLiteral not 'import'. got=%q", s.TokenLiteral())
		return false
	}

	importStmt, ok := s.(*ast.ImportStatement)
	if !ok {
		t.Errorf("s not *ast.ImportStatement. got=%T", s)
		return false
	}

	if importStmt.Path.String() != path {
		t.Errorf("importStmt.Path.String() not %q. got=%q", path, importStmt.Path.String())
		return false
	}

	if importStmt.Alias.String() != alias {
		t.Errorf("importStmt.Alias.String() not %q. got=%q", alias, importStmt.Alias.String())
		return false
	}

	return true
}

func TestSchemaDefinitions(t *testing.T) {
	input := `
schema User {
    id: int
    name: string
}

schema Admin = User
`
	l := lexer.New(input)
	p := New(l)
	program := p.Parse()
	checkParserErrors(t, p)

	if len(program.Statements) != 2 {
		t.Fatalf("program.Statements does not contain 2 statements. got=%d", len(program.Statements))
	}

	// Test User schema
	userStmt, ok := program.Statements[0].(*ast.SchemaDefinition)
	if !ok {
		t.Fatalf("stmt[0] not *ast.SchemaDefinition. got=%T", program.Statements[0])
	}
	if userStmt.Name.Value != "User" {
		t.Errorf("userStmt.Name.Value not 'User'. got=%q", userStmt.Name.Value)
	}
	if userStmt.Block == nil {
		t.Fatalf("userStmt.Block is nil")
	}

	// Test Admin schema
	adminStmt, ok := program.Statements[1].(*ast.SchemaDefinition)
	if !ok {
		t.Fatalf("stmt[1] not *ast.SchemaDefinition. got=%T", program.Statements[1])
	}
	if adminStmt.Name.Value != "Admin" {
		t.Errorf("adminStmt.Name.Value not 'Admin'. got=%q", adminStmt.Name.Value)
	}
	if adminStmt.Ref == nil || adminStmt.Ref.Name.Value != "User" {
		t.Errorf("adminStmt.Ref not 'User'. got=%v", adminStmt.Ref)
	}
}

func TestWorkflowDefinition(t *testing.T) {
	input := `
workflow main ?onErr {
    getData
    | process?localErr
    | save
    > result
}
`
	l := lexer.New(input)
	p := New(l)
	program := p.Parse()
	checkParserErrors(t, p)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	wd, ok := program.Statements[0].(*ast.WorkflowDefinition)
	if !ok {
		t.Fatalf("stmt not *ast.WorkflowDefinition. got=%T", program.Statements[0])
	}

	if wd.Name.Value != "main" {
		t.Errorf("wd.Name.Value not 'main'. got=%q", wd.Name.Value)
	}

	if wd.TrapHandler.Value != "onErr" {
		t.Errorf("wd.TrapHandler.Value not 'onErr'. got=%q", wd.TrapHandler.Value)
	}

	if len(wd.Statements) != 1 {
		t.Fatalf("wd.Statements should have 1 pipeline. got=%d", len(wd.Statements))
	}

	pipeline := wd.Statements[0]
	pc, ok := pipeline.Expression.(*ast.PipeChain)
	if !ok {
		t.Fatalf("pipeline.Expression not *ast.PipeChain. got=%T", pipeline.Expression)
	}

	if len(pc.Calls) != 3 {
		t.Errorf("pc.Calls length not 3. got=%d", len(pc.Calls))
	}

	if pipeline.Assignment.Value != "result" {
		t.Errorf("pipeline.Assignment not 'result'. got=%q", pipeline.Assignment.Value)
	}
}

func TestHandlers(t *testing.T) {
	input := `
handler onErr {
    * logError | notify
}
`
	l := lexer.New(input)
	p := New(l)
	program := p.Parse()
	checkParserErrors(t, p)

	if len(program.Statements) != 1 {
		t.Fatalf("program.Statements does not contain 1 statement. got=%d", len(program.Statements))
	}

	hd, ok := program.Statements[0].(*ast.HandlerDefinition)
	if !ok {
		t.Fatalf("stmt not *ast.HandlerDefinition. got=%T", program.Statements[0])
	}

	if hd.Name.Value != "onErr" {
		t.Errorf("hd.Name.Value not 'onErr'. got=%q", hd.Name.Value)
	}

	if len(hd.Statements) != 2 {
		t.Fatalf("hd.Statements length not 2. got=%d", len(hd.Statements))
	}
}

func TestBindings(t *testing.T) {
	input := `
resource db = postgres.connect <host=localhost> { port: 5432 }
step process : User -> void = transformer.map
`
	l := lexer.New(input)
	p := New(l)
	program := p.Parse()
	checkParserErrors(t, p)

	if len(program.Statements) != 2 {
		t.Fatalf("program.Statements does not contain 2 statements. got=%d", len(program.Statements))
	}

	rb, ok := program.Statements[0].(*ast.ResourceBinding)
	if !ok {
		t.Fatalf("stmt[0] not *ast.ResourceBinding. got=%T", program.Statements[0])
	}
	if rb.Name.Value != "db" {
		t.Errorf("rb.Name.Value not 'db'. got=%q", rb.Name.Value)
	}

	sb, ok := program.Statements[1].(*ast.StepBinding)
	if !ok {
		t.Fatalf("stmt[1] not *ast.StepBinding. got=%T", program.Statements[1])
	}
	if sb.Name.Value != "process" {
		t.Errorf("sb.Name.Value not 'process'. got=%q", sb.Name.Value)
	}
}

func checkParserErrors(t *testing.T, p *Parser) {
	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))
	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}
	t.FailNow()
}
