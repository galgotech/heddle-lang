package lsp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestGetSyntaxDiagnostics(t *testing.T) {
	tests := []struct {
		name     string
		errs     []parser.ParserError
		expected []protocol.Diagnostic
	}{
		{
			name: "single syntax error",
			errs: []parser.ParserError{
				{Message: "unexpected token 'foo'", Range: ast.Range{Start: ast.Position{Line: 1, Col: 10}, End: ast.Position{Line: 1, Col: 13}}},
			},
			expected: []protocol.Diagnostic{
				{
					Range: protocol.Range{
						Start: protocol.Position{Line: 0, Character: 9},
						End:   protocol.Position{Line: 0, Character: 12},
					},
					Severity: protocol.DiagnosticSeverityError,
					Source:   "heddle-parser",
					Message:  "unexpected token 'foo'",
				},
			},
		},
		{
			name: "multiple syntax errors",
			errs: []parser.ParserError{
				{Message: "error A", Range: ast.Range{Start: ast.Position{Line: 2, Col: 1}, End: ast.Position{Line: 2, Col: 2}}},
				{Message: "error B", Range: ast.Range{Start: ast.Position{Line: 10, Col: 5}, End: ast.Position{Line: 10, Col: 6}}},
			},
			expected: []protocol.Diagnostic{
				{
					Range:    protocol.Range{Start: protocol.Position{Line: 1, Character: 0}, End: protocol.Position{Line: 1, Character: 1}},
					Severity: protocol.DiagnosticSeverityError,
					Source:   "heddle-parser",
					Message:  "error A",
				},
				{
					Range:    protocol.Range{Start: protocol.Position{Line: 9, Character: 4}, End: protocol.Position{Line: 9, Character: 5}},
					Severity: protocol.DiagnosticSeverityError,
					Source:   "heddle-parser",
					Message:  "error B",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getSyntaxDiagnostics(tt.errs)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestGetSemanticDiagnostics_Basics(t *testing.T) {
	logger := zap.NewNop()
	s := NewServer(logger, "localhost:50051")
	ctx := context.Background()
	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	t.Run("Empty Program handles gracefully", func(t *testing.T) {
		prog := ast.ProgramNode{}
		diagnostics := getSemanticDiagnostics(ctx, s, prog, astCtx)
		// Even if the Control Plane is unreachable, it should return an empty slice rather than crashing
		assert.NotNil(t, diagnostics)
		assert.Len(t, diagnostics, 0)
	})
}

func TestValidSyntaxValidation(t *testing.T) {
	source := `import "std/io" io

handler error_print {
  * 
    | io.print
}

workflow hello_world ? error_print {
  []
    | io.print
}`

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(source)
	p := parser.New(l, astCtx)
	_ = p.Parse()

	diagnostics := getSyntaxDiagnostics(p.Errors())
	assert.Empty(t, diagnostics, "Should have no syntax diagnostics for valid code")
}
