package lsp

import (
	"context"

	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// getSyntaxDiagnostics converts parser errors into LSP diagnostic format.
func getSyntaxDiagnostics(parserErrors []parser.ParserError) []protocol.Diagnostic {
	diagnostics := make([]protocol.Diagnostic, 0, len(parserErrors))
	for _, err := range parserErrors {
		diagnostics = append(diagnostics, protocol.Diagnostic{
			Range: protocol.Range{
				Start: protocol.Position{Line: err.Range.Start.Line - 1, Character: err.Range.Start.Col - 1},
				End:   protocol.Position{Line: err.Range.End.Line - 1, Character: err.Range.End.Col - 1},
			},
			Severity: protocol.DiagnosticSeverityError,
			Source:   "heddle-parser",
			Message:  err.Message,
		})
	}
	return diagnostics
}

// getSemanticDiagnostics performs semantic and type validation and converts errors into LSP diagnostic format.
func getSemanticDiagnostics(ctx context.Context, prog ast.ProgramNode, astCtx *ast.ASTContext, registryGetter func(context.Context) (*models.RegistryInfo, error), logger *zap.Logger) []protocol.Diagnostic {
	diagnostics := []protocol.Diagnostic{}

	// Fetch schemas from Control Plane
	var steps map[string]schema.StepSchemas
	registry, err := registryGetter(ctx)
	if err != nil {
		logger.Warn("Failed to fetch registry for AOT validation", zap.Error(err))
	}
	if registry != nil {
		steps = registry.Steps
	}

	val := compiler.NewValidator(prog, astCtx, steps)
	if errs := val.ValidateAll(); len(errs) > 0 {
		for _, vErr := range errs {
			severity := protocol.DiagnosticSeverityError
			switch vErr.Severity {
			case compiler.SeverityWarning:
				severity = protocol.DiagnosticSeverityWarning
			case compiler.SeverityInformation:
				severity = protocol.DiagnosticSeverityInformation
			case compiler.SeverityHint:
				severity = protocol.DiagnosticSeverityHint
			}

			tags := []protocol.DiagnosticTag{}
			for _, t := range vErr.Tags {
				switch t {
				case compiler.TagUnnecessary:
					tags = append(tags, protocol.DiagnosticTagUnnecessary)
				case compiler.TagDeprecated:
					tags = append(tags, protocol.DiagnosticTagDeprecated)
				}
			}

			diagnostics = append(diagnostics, protocol.Diagnostic{
				Range: protocol.Range{
					Start: protocol.Position{Line: vErr.Range.Start.Line - 1, Character: vErr.Range.Start.Col - 1},
					End:   protocol.Position{Line: vErr.Range.End.Line - 1, Character: vErr.Range.End.Col - 1},
				},
				Severity: severity,
				Tags:     tags,
				Source:   "heddle-compiler",
				Message:  vErr.Message,
			})
		}
	}
	return diagnostics
}
