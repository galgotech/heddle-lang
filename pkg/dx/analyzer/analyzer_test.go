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
import "stdlib" as std

schema User {
    id: int
    name: string
}

step process: User -> User = std.Process

workflow main {
    read_data
    | process
    | unknown_step
    | process > result
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
	// We'll pass nil for the registry to trigger "Step not defined" for everything not in StepBindings.
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
	// In the workflow block, 'process' is on line 14 (1-based), but I should check the exact column.
	// Input starts with a newline, so line 1 is blank.
	// line 2: import...
	// line 12: workflow main {
	// line 13:     read_data
	// line 14:     | process
	hoverPos := ast.Position{Line: 14, Col: 7}
	hover := lsp.HandleHover(ctx, hoverPos)
	if hover != nil {
		fmt.Printf("Hover Value: %s\n", hover.Contents.Value)
	} else {
		// Try another position if 14:7 fails, let's print all call ranges
		fmt.Println("Call Ranges in AST:")
		for i, r := range ctx.CallRanges {
			call := ctx.CallNodes[i]
			fmt.Printf("Call '%s': L%d:C%d - L%d:C%d\n", ctx.GetString(call.NameRef), r.Start.Line, r.Start.Col, r.End.Line, r.End.Col)
		}
	}
}
