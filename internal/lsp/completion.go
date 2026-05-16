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

func HandleCompletion(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error), logger *zap.Logger) error {
	var params protocol.CompletionParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	content, ok := files.Load(params.TextDocument.URI)
	if !ok {
		return reply(ctx, protocol.CompletionList{}, nil)
	}
	source := content.(string)

	// Fetch registry from Control Plane
	registry, err := registryGetter(ctx)
	if err != nil {
		logger.Warn("Failed to get registry from control plane", zap.Error(err))
	}

	if registry == nil {
		// Fallback to empty registry if disconnected or error
		registry = &models.RegistryInfo{Steps: make(map[string]schema.StepSchemas)}
	}

	// Analyze context and return completion items
	items := getCompletionItems(source, params.Position, registry)

	return reply(ctx, protocol.CompletionList{
		IsIncomplete: false,
		Items:        items,
	}, nil)
}

func getCompletionItems(source string, pos protocol.Position, registry *models.RegistryInfo) []protocol.CompletionItem {
	// 1. Acquire AST context and parse (Partial parse is fine)
	ctxAST := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctxAST)

	l := lexer.New(source)
	p := parser.New(l, ctxAST)
	_ = p.Parse()

	// 2. Identify context based on line/column
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

	// Check if we are inside a workflow or handler to suggest steps
	inWorkflow := false
	for i := 1; i < len(ctxAST.WorkflowRanges); i++ {
		r := ctxAST.WorkflowRanges[i]
		if pos.Line+1 >= r.Start.Line && pos.Line+1 <= r.End.Line {
			inWorkflow = true
			break
		}
	}

	// Architectural Suggestions: Namespaces and Steps
	if inWorkflow || strings.TrimSpace(prefix) == "" {
		if strings.HasSuffix(prefix, ".") {
			// Suggest steps in namespace
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
			// Suggest namespaces
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

func getLastWord(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	return fields[len(fields)-1]
}
