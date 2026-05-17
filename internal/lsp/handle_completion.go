package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
	"github.com/galgotech/heddle-lang/pkg/schema"
)

// HandleCompletion processes a "textDocument/completion" LSP request to retrieve completion candidates.
// It parses the active document context, determines if the cursor is within a suggestable region,
// and queries the step registry fetched from the Control Plane to return matching namespaces or steps.
func HandleCompletion(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error), logger *zap.Logger) error {
	var params protocol.CompletionParams
	// Unmarshal the incoming request parameters to extract the file URI and cursor position.
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	// Load the document content from the in-memory files cache.
	content, ok := files.Load(params.TextDocument.URI)
	if !ok {
		return reply(ctx, protocol.CompletionList{}, nil)
	}
	source := content.(string)

	// Fetch the available step registry from the Control Plane to dynamically suggest connectors/steps.
	// If the control plane is unreachable or returns an error, gracefully fallback to an empty registry.
	registry, err := registryGetter(ctx)
	if err != nil {
		logger.Warn("Failed to get registry from control plane", zap.Error(err))
	}

	if registry == nil {
		// Fallback to an empty registry configuration to allow basic parsing and context suggestions.
		registry = &models.RegistryInfo{Steps: make(map[string]schema.StepSchemas)}
	}

	// Analyze the current file source and cursor position to generate relevant completion suggestions.
	items := getCompletionItems(source, params.Position, registry)

	return reply(ctx, protocol.CompletionList{
		IsIncomplete: false,
		Items:        items,
	}, nil)
}

// getCompletionItems parses the source code, identifies the lexical context at the cursor,
// and queries the step registry to provide context-aware suggestions (namespaces vs. steps).
func getCompletionItems(source string, pos protocol.Position, registry *models.RegistryInfo) []protocol.CompletionItem {
	// Acquire a pooled, pointerless AST context to minimize GC allocation overhead during parsing.
	// Perform a partial parse of the current document state to identify workflow and handler boundaries.
	ctxAST := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctxAST)

	l := lexer.New(source)
	p := parser.New(l, ctxAST)
	_ = p.Parse()

	// Identify the current line and slice the prefix text up to the cursor's character position.
	lines := strings.Split(source, "\n")
	if int(pos.Line) >= len(lines) {
		return []protocol.CompletionItem{}
	}
	line := lines[pos.Line]
	prefix := ""
	if int(pos.Character) <= len(line) {
		prefix = line[:pos.Character]
	}

	items := []protocol.CompletionItem{}

	// Verify if the cursor is physically positioned within any defined workflow or handler boundary in the AST.
	inWorkflow := false
	for i := 1; i < len(ctxAST.WorkflowRanges); i++ {
		r := ctxAST.WorkflowRanges[i]
		if pos.Line+1 >= r.Start.Line && pos.Line+1 <= r.End.Line {
			inWorkflow = true
			break
		}
	}

	// Provide autocompletion logic when the cursor is in a workflow or at an empty line.
	if inWorkflow || strings.TrimSpace(prefix) == "" {
		if strings.HasSuffix(prefix, ".") {
			// If the cursor immediately follows a dot separator, extract the namespace and suggest matching steps.
			lastWord := getLastWord(prefix)
			ns := strings.TrimSuffix(lastWord, ".")
			for cap := range registry.Steps {
				if strings.HasPrefix(cap, ns+".") {
					stepName := strings.TrimPrefix(cap, ns+".")
					items = append(items, protocol.CompletionItem{
						Label:            stepName,
						Kind:             protocol.CompletionItemKindFunction,
						Detail:           "Heddle Step",
						InsertText:       stepName,
						InsertTextFormat: protocol.InsertTextFormatPlainText,
					})
				}
			}
		} else {
			// If the cursor does not follow a dot, suggest all top-level namespaces derived from the registered steps.
			namespaces := make(map[string]bool)
			for cap := range registry.Steps {
				parts := strings.Split(cap, ".")
				if len(parts) > 1 {
					namespaces[parts[0]] = true
				}
			}
			for ns := range namespaces {
				items = append(items, protocol.CompletionItem{
					Label:  ns,
					Kind:   protocol.CompletionItemKindModule,
					Detail: "Namespace",
				})
			}
		}
	}

	return items
}

// getLastWord returns the trailing non-whitespace word from the given input string,
// which is useful for extracting current symbol prefix details for autocompletion.
func getLastWord(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}
