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

// handleCompletion processes a "textDocument/completion" LSP request to retrieve completion candidates.
// It parses the active document context, determines if the cursor is within a suggestable region,
// and queries the step registry fetched from the Control Plane to return matching namespaces or steps.
func handleCompletion(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error), logger *zap.Logger) error {
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
	p.Parse()

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

	// Check if we are inside a resource mapping block <key=value, ...>
	langleIdx := strings.LastIndex(prefix, "<")
	if langleIdx >= 0 {
		// Check if the '<' is not closed by a '>' on the same line prefix
		if !strings.Contains(prefix[langleIdx:], ">") {
			// We are inside a resource mapping block!
			// Check if we are after an '=' in the current mapping (e.g. '<connection=' or '<key=val, connection=')
			lastComma := strings.LastIndex(prefix[langleIdx:], ",")
			currentMapping := prefix[langleIdx:]
			if lastComma >= 0 {
				currentMapping = prefix[langleIdx+lastComma:]
			}
			if strings.Contains(currentMapping, "=") {
				// User is typing the value part of a key=value mapping, so suggest resource names!
				for idx := 1; idx < len(ctxAST.ResourceNodes); idx++ {
					res := ctxAST.ResourceNodes[idx]
					resName := ctxAST.GetString(res.NameRef)
					if resName != "" {
						items = append(items, protocol.CompletionItem{
							Label:  resName,
							Kind:   protocol.CompletionItemKindStruct,
							Detail: "Local Resource",
						})
					}
				}
				return items
			}
		}
	}

	// Verify if the cursor is physically positioned within any defined workflow boundary in the AST.
	inWorkflow := false
	var activeWorkflow *ast.WorkflowNode
	for i := 1; i < len(ctxAST.WorkflowRanges); i++ {
		r := ctxAST.WorkflowRanges[i]
		if pos.Line+1 >= r.Start.Line && pos.Line+1 <= r.End.Line {
			inWorkflow = true
			activeWorkflow = &ctxAST.WorkflowNodes[i]
			break
		}
	}

	// Verify if the cursor is physically positioned within any defined handler boundary in the AST.
	inHandler := false
	var activeHandler *ast.HandlerNode
	for i := 1; i < len(ctxAST.HandlerRanges); i++ {
		r := ctxAST.HandlerRanges[i]
		if pos.Line+1 >= r.Start.Line && pos.Line+1 <= r.End.Line {
			inHandler = true
			activeHandler = &ctxAST.HandlerNodes[i]
			break
		}
	}

	// Provide autocompletion logic when the cursor is in a workflow/handler or at an empty line.
	if inWorkflow || inHandler || strings.TrimSpace(prefix) == "" {
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
			// Suggest namespaces from registry
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

			// Suggest imported aliases / namespaces (e.g. io)
			for idx := 1; idx < len(ctxAST.ImportNodes); idx++ {
				imp := ctxAST.ImportNodes[idx]
				alias := ctxAST.GetString(imp.AliasRef)
				if alias == "" {
					path := strings.Trim(ctxAST.GetString(imp.PathRef), `"'`)
					parts := strings.Split(path, "/")
					if len(parts) > 0 {
						alias = parts[len(parts)-1]
					}
				}
				if alias != "" {
					items = append(items, protocol.CompletionItem{
						Label:  alias,
						Kind:   protocol.CompletionItemKindModule,
						Detail: "Imported Namespace",
					})
				}
			}

			// Suggest steps defined/created in the same file .he
			for idx := 1; idx < len(ctxAST.StepBindingNodes); idx++ {
				sb := ctxAST.StepBindingNodes[idx]
				stepName := ctxAST.GetString(sb.NameRef)
				if stepName != "" {
					items = append(items, protocol.CompletionItem{
						Label:  stepName,
						Kind:   protocol.CompletionItemKindFunction,
						Detail: "Local Step Binding",
					})
				}
			}

			// Suggest variables/assignments created inside same workflow
			if activeWorkflow != nil {
				for sIdx := activeWorkflow.StatementRefsStart; sIdx < activeWorkflow.StatementRefsEnd; sIdx++ {
					stmtRef := ctxAST.StatementRefs[sIdx]
					if stmtRef == ast.NilNode {
						continue
					}
					stmt := ctxAST.PipelineStatementNodes[stmtRef]
					assignName := ctxAST.GetString(stmt.AssignmentRef)
					if assignName != "" {
						items = append(items, protocol.CompletionItem{
							Label:  assignName,
							Kind:   protocol.CompletionItemKindVariable,
							Detail: "Workflow Variable",
						})
					}
				}
			}

			if activeHandler != nil {
				for sIdx := activeHandler.HandlerStatementRefsStart; sIdx < activeHandler.HandlerStatementRefsEnd; sIdx++ {
					hsRef := ctxAST.HandlerStatementRefs[sIdx]
					if hsRef == ast.NilNode {
						continue
					}
					hs := ctxAST.HandlerStatementNodes[hsRef]
					if hs.StmtRef == ast.NilNode {
						continue
					}
					stmt := ctxAST.PipelineStatementNodes[hs.StmtRef]
					assignName := ctxAST.GetString(stmt.AssignmentRef)
					if assignName != "" {
						items = append(items, protocol.CompletionItem{
							Label:  assignName,
							Kind:   protocol.CompletionItemKindVariable,
							Detail: "Handler Variable",
						})
					}
				}
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
