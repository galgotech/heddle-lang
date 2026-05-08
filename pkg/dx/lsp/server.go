package lsp

import (
	"encoding/json"
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/dx/analyzer"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
)

// Diagnostic structure for LSP (textDocument/publishDiagnostics)
type LSPDiagnostic struct {
	Range    LSPRange `json:"range"`
	Severity int      `json:"severity"`
	Source   string   `json:"source"`
	Message  string   `json:"message"`
}

type LSPRange struct {
	Start LSPPosition `json:"start"`
	End   LSPPosition `json:"end"`
}

type LSPPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// PublishDiagnostics converts analyzer diagnostics to LSP format.
func PublishDiagnostics(uri string, diagnostics []analyzer.Diagnostic) string {
	lspDiagnostics := make([]LSPDiagnostic, len(diagnostics))
	for i, d := range diagnostics {
		severity := 1 // Error
		if d.Severity == analyzer.Warning {
			severity = 2
		}

		lspDiagnostics[i] = LSPDiagnostic{
			Range: LSPRange{
				Start: LSPPosition{Line: int(d.Range.Start.Line) - 1, Character: int(d.Range.Start.Col)},
				End:   LSPPosition{Line: int(d.Range.End.Line) - 1, Character: int(d.Range.End.Col)},
			},
			Severity: severity,
			Source:   "heddle-analyzer",
			Message:  d.Message,
		}
		if d.Help != "" {
			lspDiagnostics[i].Message += "\n\nHelp: " + d.Help
		}
	}

	params := map[string]any{
		"uri":         uri,
		"diagnostics": lspDiagnostics,
	}

	notification := map[string]any{
		"jsonrpc": "2.0",
		"method":  "textDocument/publishDiagnostics",
		"params":  params,
	}

	raw, _ := json.Marshal(notification)
	return string(raw)
}

// HoverResponse for textDocument/hover
type HoverResponse struct {
	Contents HoverContents `json:"contents"`
	Range    *LSPRange     `json:"range,omitempty"`
}

type HoverContents struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

// HandleHover generates a hover response for a given position in the AST.
func HandleHover(ctx *ast.ASTContext, pos ast.Position) *HoverResponse {
	// Find the node at the position.
	// For simplicity, we'll check StepBindings and CallNodes.

	// Check CallNodes
	for i, r := range ctx.CallRanges {
		if isInside(r, pos) {
			call := ctx.CallNodes[i]
			name := ctx.GetString(call.NameRef)

			return &HoverResponse{
				Contents: HoverContents{
					Kind:  "markdown",
					Value: fmt.Sprintf("### Step: `%s`\n---\nExecutes the %s step.\n\n**Expected Schema:** `*core.Table`\n**Locality:** Remote (Python Worker)", name, name),
				},
				Range: &LSPRange{
					Start: LSPPosition{Line: int(r.Start.Line) - 1, Character: int(r.Start.Col)},
					End:   LSPPosition{Line: int(r.End.Line) - 1, Character: int(r.End.Col)},
				},
			}
		}
	}

	return nil
}

func isInside(r ast.Range, p ast.Position) bool {
	if p.Line < r.Start.Line || p.Line > r.End.Line {
		return false
	}
	if p.Line == r.Start.Line && p.Col < r.Start.Col {
		return false
	}
	if p.Line == r.End.Line && p.Col > r.End.Col {
		return false
	}
	return true
}
