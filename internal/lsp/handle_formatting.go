package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

func handleFormatting(ctx context.Context, reply jsonrpc2.Replier, req jsonrpc2.Request, files *sync.Map) error {
	var params protocol.DocumentFormattingParams
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

	f := NewFormatter(astCtx)
	f.Source = text.(string)
	formatted := f.Format(prog)

	return reply(ctx, []protocol.TextEdit{
		{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: 100000, Character: 0},
			},
			NewText: formatted,
		},
	}, nil)
}

// Formatter handles pretty-printing of Heddle source code.
type Formatter struct {
	ctx    *ast.ASTContext
	Source string
}

func extractCommentContent(lines []string) []string {
	var content []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "//") {
			lineText := strings.TrimPrefix(trimmed, "//")
			lineText = strings.TrimPrefix(lineText, " ")
			content = append(content, lineText)
			continue
		}

		// Handle block comment line
		if strings.Contains(trimmed, "/*") {
			idx := strings.Index(trimmed, "/*")
			if strings.HasPrefix(trimmed[idx:], "/**") {
				trimmed = trimmed[:idx] + trimmed[idx+3:]
			} else {
				trimmed = trimmed[:idx] + trimmed[idx+2:]
			}
			trimmed = strings.TrimSpace(trimmed)
		}
		if strings.Contains(trimmed, "*/") {
			idx := strings.Index(trimmed, "*/")
			trimmed = trimmed[:idx] + trimmed[idx+2:]
			trimmed = strings.TrimSpace(trimmed)
		}
		if strings.HasPrefix(trimmed, "*") {
			trimmed = trimmed[1:]
			trimmed = strings.TrimPrefix(trimmed, " ")
		}
		if trimmed != "" || len(content) > 0 {
			content = append(content, trimmed)
		}
	}
	for len(content) > 0 && content[len(content)-1] == "" {
		content = content[:len(content)-1]
	}
	return content
}

func formatAsStandardBlockComment(content []string) string {
	if len(content) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("/**\n")
	for _, line := range content {
		sb.WriteString(" * ")
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	sb.WriteString(" */")
	return sb.String()
}

func (f *Formatter) getCommentsAbove(line uint32) (string, string) {
	if f.Source == "" || line <= 1 {
		return "", ""
	}
	lines := strings.Split(f.Source, "\n")
	if int(line) > len(lines) {
		return "", ""
	}

	var collected []string
	inBlockComment := false

	for i := int(line) - 2; i >= 0; i-- {
		l := lines[i]
		trimmed := strings.TrimSpace(l)

		if inBlockComment {
			collected = append([]string{l}, collected...)
			if strings.Contains(trimmed, "/*") {
				inBlockComment = false
				break
			}
			continue
		}

		if trimmed == "" {
			if len(collected) > 0 {
				hasBlockComment := false
				for _, col := range collected {
					colTrim := strings.TrimSpace(col)
					if strings.HasPrefix(colTrim, "*") || strings.Contains(colTrim, "/*") || strings.Contains(colTrim, "*/") {
						hasBlockComment = true
						break
					}
				}
				if hasBlockComment {
					collected = append([]string{l}, collected...)
					continue
				}
				break
			}
			continue
		}

		isComment := false
		if strings.HasPrefix(trimmed, "//") {
			isComment = true
		} else if strings.HasPrefix(trimmed, "*") {
			isComment = true
		} else if strings.Contains(trimmed, "/*") {
			isComment = true
		} else if strings.Contains(trimmed, "*/") {
			isComment = true
		}

		if isComment {
			collected = append([]string{l}, collected...)
			if strings.Contains(trimmed, "*/") {
				inBlockComment = true
				if strings.Contains(trimmed, "/*") {
					inBlockComment = false
					break
				}
			} else if strings.Contains(trimmed, "/*") {
				break
			}
			continue
		}

		break
	}

	if len(collected) == 0 {
		return "", ""
	}

	var originalBuilder strings.Builder
	for i, l := range collected {
		originalBuilder.WriteString(l)
		if i < len(collected)-1 {
			originalBuilder.WriteString("\n")
		}
	}
	originalStr := originalBuilder.String()

	content := extractCommentContent(collected)
	return formatAsStandardBlockComment(content), originalStr
}

// Format takes a ProgramNode and returns a formatted string.
func (f *Formatter) Format(program ast.ProgramNode) string {
	var sb strings.Builder
	usedComments := make(map[string]bool)

	// Pre-extract comments to populate usedComments before writing 0. Comments
	type commentInfo struct {
		formatted string
		original  string
	}
	resourceComments := make(map[uint32]commentInfo)
	if program.ResourceRefsEnd > program.ResourceRefsStart {
		for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
			ref := f.ctx.ResourceRefs[i]
			rRange := f.ctx.ResourceRanges[ref]
			if comment, original := f.getCommentsAbove(rRange.Start.Line); comment != "" {
				resourceComments[i] = commentInfo{formatted: comment, original: original}
				usedComments[strings.TrimSpace(original)] = true
			}
		}
	}

	stepComments := make(map[uint32]commentInfo)
	if program.StepRefsEnd > program.StepRefsStart {
		for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
			ref := f.ctx.StepRefs[i]
			sRange := f.ctx.StepRanges[ref]
			if comment, original := f.getCommentsAbove(sRange.Start.Line); comment != "" {
				stepComments[i] = commentInfo{formatted: comment, original: original}
				usedComments[strings.TrimSpace(original)] = true
			}
		}
	}

	workflowComments := make(map[uint32]commentInfo)
	if program.WorkflowRefsEnd > program.WorkflowRefsStart {
		for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
			ref := f.ctx.WorkflowRefs[i]
			wRange := f.ctx.WorkflowRanges[ref]
			if comment, original := f.getCommentsAbove(wRange.Start.Line); comment != "" {
				workflowComments[i] = commentInfo{formatted: comment, original: original}
				usedComments[strings.TrimSpace(original)] = true
			}
		}
	}

	// 0. Comments
	if program.CommentRefsEnd > program.CommentRefsStart {
		for i := program.CommentRefsStart; i < program.CommentRefsEnd; i++ {
			ref := f.ctx.CommentRefs[i]
			node := f.ctx.CommentNodes[ref]
			commentVal := f.ctx.GetString(node.ValueRef)
			trimmedVal := strings.TrimSpace(commentVal)
			if !usedComments[trimmedVal] {
				sb.WriteString(commentVal)
				sb.WriteString("\n\n")
			}
		}
	}

	// 1. Imports
	if program.ImportRefsEnd > program.ImportRefsStart {
		var importRefs []ast.NodeRef
		for i := program.ImportRefsStart; i < program.ImportRefsEnd; i++ {
			importRefs = append(importRefs, f.ctx.ImportRefs[i])
		}
		sort.Slice(importRefs, func(i, j int) bool {
			pathI := f.ctx.GetString(f.ctx.ImportNodes[importRefs[i]].PathRef)
			pathJ := f.ctx.GetString(f.ctx.ImportNodes[importRefs[j]].PathRef)
			return pathI < pathJ
		})
		for _, ref := range importRefs {
			node := f.ctx.ImportNodes[ref]
			if node.AliasRef.Start != node.AliasRef.End {
				fmt.Fprintf(&sb, "import \"%s\" %s\n", f.ctx.GetString(node.PathRef), f.ctx.GetString(node.AliasRef))
			} else {
				fmt.Fprintf(&sb, "import \"%s\"\n", f.ctx.GetString(node.PathRef))
			}
		}
		sb.WriteString("\n")
	}

	// 2. Resources
	if program.ResourceRefsEnd > program.ResourceRefsStart {
		for i := program.ResourceRefsStart; i < program.ResourceRefsEnd; i++ {
			ref := f.ctx.ResourceRefs[i]
			node := f.ctx.ResourceNodes[ref]
			if info, exists := resourceComments[i]; exists {
				sb.WriteString(info.formatted)
				sb.WriteString("\n")
			}
			fmt.Fprintf(&sb, "resource %s = ", f.ctx.GetString(node.NameRef))
			f.writeFunctionRef(&sb, node.FunctionRef, 0)
			sb.WriteString("\n\n")
		}
	}

	// 3. Steps
	if program.StepRefsEnd > program.StepRefsStart {
		for i := program.StepRefsStart; i < program.StepRefsEnd; i++ {
			ref := f.ctx.StepRefs[i]
			node := f.ctx.StepBindingNodes[ref]
			if info, exists := stepComments[i]; exists {
				sb.WriteString(info.formatted)
				sb.WriteString("\n")
			}
			fmt.Fprintf(&sb, "step %s = ", f.ctx.GetString(node.NameRef))
			f.writeFunctionRef(&sb, node.FunctionRef, 0)
			sb.WriteString("\n\n")
		}
	}

	// 4. Handlers
	if program.HandlerRefsEnd > program.HandlerRefsStart {
		for i := program.HandlerRefsStart; i < program.HandlerRefsEnd; i++ {
			ref := f.ctx.HandlerRefs[i]
			node := f.ctx.HandlerNodes[ref]
			fmt.Fprintf(&sb, "handler %s {\n", f.ctx.GetString(node.NameRef))
			for j := node.HandlerStatementRefsStart; j < node.HandlerStatementRefsEnd; j++ {
				hsRef := f.ctx.HandlerStatementRefs[j]
				hs := f.ctx.HandlerStatementNodes[hsRef]
				if hs.IsCatchAll {
					sb.WriteString("  *\n")
				}
				if j > node.HandlerStatementRefsStart {
					sb.WriteString("\n")
				}
				ps := f.ctx.PipelineStatementNodes[hs.StmtRef]
				f.writePipelineStatement(&sb, ps, 1)
			}
			sb.WriteString("}\n\n")
		}
	}

	// 5. Workflows
	if program.WorkflowRefsEnd > program.WorkflowRefsStart {
		for i := program.WorkflowRefsStart; i < program.WorkflowRefsEnd; i++ {
			ref := f.ctx.WorkflowRefs[i]
			node := f.ctx.WorkflowNodes[ref]
			if info, exists := workflowComments[i]; exists {
				sb.WriteString(info.formatted)
				sb.WriteString("\n")
			}
			fmt.Fprintf(&sb, "workflow %s ", f.ctx.GetString(node.NameRef))
			if node.TrapRef.Start != node.TrapRef.End {
				fmt.Fprintf(&sb, "? %s ", f.ctx.GetString(node.TrapRef))
			}
			sb.WriteString("{\n")
			for j := node.StatementRefsStart; j < node.StatementRefsEnd; j++ {
				if j > node.StatementRefsStart {
					sb.WriteString("\n")
				}
				psRef := f.ctx.StatementRefs[j]
				ps := f.ctx.PipelineStatementNodes[psRef]
				f.writePipelineStatement(&sb, ps, 1)
			}
			sb.WriteString("}\n\n")
		}
	}

	return strings.TrimSpace(sb.String()) + "\n"
}

func (f *Formatter) writeFunctionRef(sb *strings.Builder, ref ast.NodeRef, indent int) {
	node := f.ctx.FunctionRefNodes[ref]

	// Resource Mapping
	if node.ResourcesRefRef != 0 {
		sb.WriteString("<")
		rr := f.ctx.ResourceRefNodes[node.ResourcesRefRef]
		for i := rr.MappingsRefStart; i < rr.MappingsRefEnd; i++ {
			mRef := f.ctx.MappingRefs[i]
			m := f.ctx.ResourceMappingNodes[mRef]
			fmt.Fprintf(sb, "%s=%s", f.ctx.GetString(m.KeyRef), f.ctx.GetString(m.ValueRef))
			if i < rr.MappingsRefEnd-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString("> ")
	}

	if node.ModuleRef.Start != node.ModuleRef.End {
		fmt.Fprintf(sb, "%s.%s", f.ctx.GetString(node.ModuleRef), f.ctx.GetString(node.NameRef))
	} else {
		sb.WriteString(f.ctx.GetString(node.NameRef))
	}

	// Config
	if node.ConfigRef != 0 {
		sb.WriteString(" ")
		f.writeDict(sb, node.ConfigRef, indent)
	}
}

func (f *Formatter) writeDict(sb *strings.Builder, ref ast.NodeRef, indent int) {
	node := f.ctx.DictNodes[ref]
	r := f.ctx.DictRanges[ref]
	isMultiline := r.Start.Line != r.End.Line

	if !isMultiline {
		sb.WriteString("{ ")
		for i := node.PairRefsStart; i < node.PairRefsEnd; i++ {
			pRef := f.ctx.PairRefs[i]
			p := f.ctx.PairNodes[pRef]
			fmt.Fprintf(sb, "%s: ", f.ctx.GetString(p.KeyRef))
			f.writeLiteral(sb, p.ValueRef, indent)
			if i < node.PairRefsEnd-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" }")
		return
	}

	sb.WriteString("{\n")
	for i := node.PairRefsStart; i < node.PairRefsEnd; i++ {
		pRef := f.ctx.PairRefs[i]
		p := f.ctx.PairNodes[pRef]
		f.writeIndent(sb, indent+1)
		fmt.Fprintf(sb, "%s: ", f.ctx.GetString(p.KeyRef))
		f.writeLiteral(sb, p.ValueRef, indent+1)
		sb.WriteString("\n")
	}
	f.writeIndent(sb, indent)
	sb.WriteString("}")
}

func (f *Formatter) writeLiteral(sb *strings.Builder, ref ast.NodeRef, indent int) {
	node := f.ctx.LiteralNodes[ref]
	switch node.Type {
	case ast.LiteralString:
		fmt.Fprintf(sb, "\"%s\"", f.ctx.GetString(node.ValueRef))
	case ast.LiteralInt, ast.LiteralFloat, ast.LiteralBool, ast.LiteralNull:
		sb.WriteString(f.ctx.GetString(node.ValueRef))
	case ast.LiteralDict:
		f.writeDict(sb, node.Ref, indent)
	case ast.LiteralList:
		l := f.ctx.ListNodes[node.Ref]
		sb.WriteString("[")
		for i := l.LiteralRefsStart; i < l.LiteralRefsEnd; i++ {
			lr := f.ctx.LiteralRefs[i]
			f.writeLiteral(sb, lr, indent)
			if i < l.LiteralRefsEnd-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString("]")
	}
}

func (f *Formatter) writePipelineStatement(sb *strings.Builder, ps ast.PipelineStatementNode, indent int) {
	f.writeIndent(sb, indent)

	// Check if ExprRef is PipeChain or Dataframe
	// (This logic needs to be careful as ExprRef is just a NodeRef)
	// For now let's assume it's a PipeChain if it's within bounds
	if int(ps.ExprRef) < len(f.ctx.PipeChainNodes) {
		f.writePipeChain(sb, f.ctx.PipeChainNodes[ps.ExprRef], indent)
	}

	if ps.AssignmentRef.Start != ps.AssignmentRef.End {
		sb.WriteString("\n")
		f.writeIndent(sb, indent)
		fmt.Fprintf(sb, "> %s", f.ctx.GetString(ps.AssignmentRef))
	}
	sb.WriteString("\n")
}

func (f *Formatter) writeDataframe(sb *strings.Builder, ref ast.NodeRef, indent int) {
	node := f.ctx.DataframeNodes[ref]
	sb.WriteString("[")
	// If it has dicts, let's keep it simple for now and just put them in.
	for i := node.DictRefsStart; i < node.DictRefsEnd; i++ {
		dRef := f.ctx.DictRefs[i]
		f.writeDict(sb, dRef, indent)
		if i < node.DictRefsEnd-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
}

func (f *Formatter) writePipeChain(sb *strings.Builder, pc ast.PipeChainNode, indent int) {
	effectiveFirst := true
	skippedPlaceholder := false
	for i := pc.CallRefsStart; i < pc.CallRefsEnd; i++ {
		cRef := f.ctx.CallRefs[i]
		call := f.ctx.CallNodes[cRef]

		// Check if this is a placeholder call (implicit input)
		isEmpty := call.NameRef.End == 0 &&
			call.FunctionRef == ast.NilNode &&
			!call.IsPrql &&
			call.DataframeRef == ast.NilNode

		if isEmpty && i == pc.CallRefsStart {
			// Skip the initial placeholder call but the next call MUST have a pipe.
			skippedPlaceholder = true
			continue
		}

		pipeLevel := indent
		if !effectiveFirst || skippedPlaceholder {
			pipeLevel = indent + 1
		}

		if !effectiveFirst {
			sb.WriteString("\n")
			f.writeIndent(sb, pipeLevel)
			sb.WriteString("| ")
		} else if skippedPlaceholder {
			// If we skipped a placeholder, we are already indented at 'indent' level
			// from writePipelineStatement. We want 'indent + 1' total.
			f.writeIndent(sb, 1)
			sb.WriteString("| ")
		}

		fnIndent := pipeLevel
		if !effectiveFirst || skippedPlaceholder {
			fnIndent = pipeLevel + 1
		}
		effectiveFirst = false

		if call.IsPrql {
			sb.WriteString(f.ctx.GetString(call.QueryRef))
		} else if call.NameRef.Start != call.NameRef.End {
			sb.WriteString(f.ctx.GetString(call.NameRef))
		} else if call.FunctionRef != 0 {
			f.writeFunctionRef(sb, call.FunctionRef, fnIndent)
		} else if call.DataframeRef != 0 {
			f.writeDataframe(sb, call.DataframeRef, fnIndent)
		}

		if call.TrapRef.Start != call.TrapRef.End {
			fmt.Fprintf(sb, " ? %s", f.ctx.GetString(call.TrapRef))
		}
	}
}

func (f *Formatter) writeIndent(sb *strings.Builder, indent int) {
	for range indent {
		sb.WriteString("  ")
	}
}

// NewFormatter creates a new Formatter.
func NewFormatter(ctx *ast.ASTContext) *Formatter {
	return &Formatter{ctx: ctx}
}
