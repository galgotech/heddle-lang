package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleHover(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map, registryGetter func(context.Context) (*models.RegistryInfo, error)) error {
	var params protocol.HoverParams
	if err := json.Unmarshal(req.Params(), &params); err != nil {
		return reply(ctx, nil, err)
	}

	uri := params.TextDocument.URI
	text, ok := files.Load(uri)
	if !ok {
		return reply(ctx, nil, nil)
	}

	astCtx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(astCtx)

	l := lexer.New(text.(string))
	p := parser.New(l, astCtx)
	prog := p.Parse()

	nav := NewNavigator(astCtx)
	symbolName, _ := nav.SymbolAt(prog, params.Position.Line+1, params.Position.Character+1)
	if symbolName == "" {
		return reply(ctx, nil, nil)
	}

	var docString string
	var bindsTo string
	var hasLocalDef bool
	var isResource bool
	var isWorkflow bool

	// Check local steps first
	for i := prog.StepRefsStart; i < prog.StepRefsEnd; i++ {
		ref := astCtx.StepRefs[i]
		node := astCtx.StepBindingNodes[ref]
		if astCtx.GetString(node.NameRef) == symbolName {
			hasLocalDef = true
			if node.CommentRef.Start != node.CommentRef.End {
				docString = CleanBlockComment(astCtx.GetString(node.CommentRef))
			}
			// Resolve the underlying function it binds to
			frNode := astCtx.FunctionRefNodes[node.FunctionRef]
			name := astCtx.GetString(frNode.NameRef)
			if frNode.ModuleRef.Start != frNode.ModuleRef.End {
				alias := astCtx.GetString(frNode.ModuleRef)
				namespace := nav.resolveImportNamespace(prog, alias)
				bindsTo = namespace + "." + name
			} else {
				bindsTo = name
			}
			break
		}
	}

	// Check local resources if not step
	if !hasLocalDef {
		for i := prog.ResourceRefsStart; i < prog.ResourceRefsEnd; i++ {
			ref := astCtx.ResourceRefs[i]
			node := astCtx.ResourceNodes[ref]
			if astCtx.GetString(node.NameRef) == symbolName {
				hasLocalDef = true
				isResource = true
				if node.CommentRef.Start != node.CommentRef.End {
					docString = CleanBlockComment(astCtx.GetString(node.CommentRef))
				}
				// Resolve the underlying connector it binds to
				frNode := astCtx.FunctionRefNodes[node.FunctionRef]
				name := astCtx.GetString(frNode.NameRef)
				if frNode.ModuleRef.Start != frNode.ModuleRef.End {
					alias := astCtx.GetString(frNode.ModuleRef)
					namespace := nav.resolveImportNamespace(prog, alias)
					bindsTo = namespace + "." + name
				} else {
					bindsTo = name
				}
				break
			}
		}
	}

	// Check local workflows
	if !hasLocalDef {
		for i := prog.WorkflowRefsStart; i < prog.WorkflowRefsEnd; i++ {
			ref := astCtx.WorkflowRefs[i]
			node := astCtx.WorkflowNodes[ref]
			if astCtx.GetString(node.NameRef) == symbolName {
				hasLocalDef = true
				isWorkflow = true
				break
			}
		}
	}

	// Fallback targets for external schemas in the registry
	targetName := symbolName
	if bindsTo != "" {
		targetName = bindsTo
	}

	// Also infer if external is a resource or step based on dot namespace if registry has it
	registry, _ := registryGetter(ctx)
	if !hasLocalDef && registry != nil {
		if _, ok := registry.Resources[targetName]; ok {
			isResource = true
		}
	}

	var markdown strings.Builder
	if isWorkflow {
		markdown.WriteString("### 🔄 Workflow: **" + symbolName + "**\n")
		markdown.WriteString("Orchestrated Directed Acyclic Graph (DAG) flow definition.\n\n")
	} else if isResource {
		markdown.WriteString("### 🔌 Resource: **" + symbolName + "**\n")
		if bindsTo != "" {
			markdown.WriteString("Binds to connector: `" + bindsTo + "`\n\n")
		}
	} else {
		markdown.WriteString("### ⚡ Step: **" + symbolName + "**\n")
		if bindsTo != "" {
			markdown.WriteString("Binds to: `" + bindsTo + "`\n\n")
		}
	}

	// Load documentation from registry if local is absent
	if docString == "" && registry != nil {
		if isResource {
			if res, ok := registry.Resources[targetName]; ok && res.Documentation != "" {
				docString = res.Documentation
			}
		} else {
			if step, ok := registry.Steps[targetName]; ok && step.Documentation != "" {
				docString = step.Documentation
			}
		}
	}

	if docString != "" {
		markdown.WriteString(docString + "\n\n")
	}

	// Render details if available in registry
	if registry != nil {
		if isResource {
			if res, ok := registry.Resources[targetName]; ok && len(res.Config.Fields) > 0 {
				markdown.WriteString("#### ⚙️ Configuration\n")
				markdown.WriteString("| Field | Type |\n")
				markdown.WriteString("|---|---|\n")
				for _, f := range res.Config.Fields {
					markdown.WriteString("| `" + f.Name + "` | `" + f.Type + "` |\n")
				}
				markdown.WriteString("\n")
			}
		} else {
			if step, ok := registry.Steps[targetName]; ok {
				// Config
				if len(step.Config.Fields) > 0 {
					markdown.WriteString("#### ⚙️ Configuration\n")
					markdown.WriteString("| Field | Type |\n")
					markdown.WriteString("|---|---|\n")
					for _, f := range step.Config.Fields {
						markdown.WriteString("| `" + f.Name + "` | `" + f.Type + "` |\n")
					}
					markdown.WriteString("\n")
				}

				// Input Frame
				markdown.WriteString("#### 📥 Input HeddleFrame\n")
				if len(step.Input) > 0 {
					markdown.WriteString("| Column | Arrow Type |\n")
					markdown.WriteString("|---|---|\n")
					for _, f := range step.Input {
						markdown.WriteString("| `" + f.Name + "` | `" + f.ArrowType + "` |\n")
					}
					markdown.WriteString("\n")
				}

				// Output Frame
				markdown.WriteString("#### 📤 Output HeddleFrame\n")
				if len(step.Output) > 0 {
					markdown.WriteString("| Column | Arrow Type |\n")
					markdown.WriteString("|---|---|\n")
					for _, f := range step.Output {
						markdown.WriteString("| `" + f.Name + "` | `" + f.ArrowType + "` |\n")
					}
					markdown.WriteString("\n")
				}

				// Source code preview
				if step.SourceCode != "" {
					markdown.WriteString("#### 🔍 Source Implementation\n")
					markdown.WriteString("```go\n")
					markdown.WriteString(step.SourceCode)
					markdown.WriteString("\n```\n")
				}
			}
		}
	}

	if markdown.Len() > 0 {
		return reply(ctx, protocol.Hover{
			Contents: protocol.MarkupContent{
				Kind:  protocol.Markdown,
				Value: markdown.String(),
			},
		}, nil)
	}

	return reply(ctx, nil, nil)
}
