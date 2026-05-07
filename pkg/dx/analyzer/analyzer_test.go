package analyzer_test

import (
	"fmt"
	"testing"

	"github.com/galgotech/heddle-lang/pkg/dx/analyzer"
	"github.com/galgotech/heddle-lang/pkg/dx/lsp"
	"github.com/galgotech/heddle-lang/pkg/dx/terminal"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func TestEndToEndDiagnostics(t *testing.T) {
	input := `
import "stdlib" std

step process = std.Process

workflow main {
  read_data
    | process
    | unknown_step
    | (from input select {id, val})
}
`
	l := lexer.New(input)
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	p := parser.New(l, ctx)
	program := p.Parse()

	// Collect parser errors first.
	var diagnostics []analyzer.Diagnostic
	for _, e := range p.Errors() {
		diagnostics = append(diagnostics, analyzer.Diagnostic{
			Message: "Syntax Error: " + e.Message,
			Range: ast.Range{
				Start: ast.Position{Line: uint32(e.Line), Col: uint32(e.Column)},
				End:   ast.Position{Line: uint32(e.Line), Col: uint32(e.Column + 1)}, // Approximation
			},
			Severity: analyzer.Error,
		})
	}

	// Run semantic analysis.
	ana := analyzer.New(ctx, nil)
	semanticErrors := ana.Analyze(program)
	diagnostics = append(diagnostics, semanticErrors...)

	// Demonstrate Terminal Reporter
	fmt.Println("--- Terminal Diagnostic Output ---")
	reporter := terminal.NewReporter(input)
	reporter.Report(diagnostics)

	// Demonstrate LSP Payload
	fmt.Println("--- LSP publishDiagnostics Payload ---")
	lspPayload := lsp.PublishDiagnostics("file:///test.he", diagnostics)
	fmt.Println(lspPayload)

	// Demonstrate Hover
	fmt.Println("\n--- LSP Hover Example (on 'process' in workflow) ---")
	// line 1: blank
	// line 2: import...
	// line 3: blank
	// line 4: step process...
	// line 5: blank
	// line 6: workflow main {
	// line 7:   read_data
	// line 8:     | process
	hoverPos := ast.Position{Line: 8, Col: 7}
	hover := lsp.HandleHover(ctx, hoverPos)
	if hover != nil {
		fmt.Printf("Hover Value: %s\n", hover.Contents.Value)
	} else {
		fmt.Println("Call Ranges in AST:")
		for i, r := range ctx.CallRanges {
			if i == 0 {
				continue
			}
			call := ctx.CallNodes[i]
			name := ctx.GetString(call.NameRef)
			if call.IsPrql {
				name = "PRQL"
			}
			fmt.Printf("Call '%s': L%d:C%d - L%d:C%d\n", name, r.Start.Line, r.Start.Col, r.End.Line, r.End.Col)
		}
	}
}
