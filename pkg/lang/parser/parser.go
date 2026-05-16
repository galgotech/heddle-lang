// Package parser implements a recursive descent parser for the Heddle DSL.
// It transforms a stream of tokens into a structured Abstract Syntax Tree (AST).
package parser

import (
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

// ParserError represents a diagnostic error encountered during the parsing phase.
type ParserError struct {
	Message string    // Human-readable description of the error.
	Range   ast.Range // Source range where the error occurred.
}

// Parser maintains the state of the parsing process, including the lexer,
// lookahead tokens, and the AST context for node allocation.
type Parser struct {
	l                *lexer.Lexer      // Source of tokens.
	curToken         lexer.Token       // Current token being processed.
	peekTokens       []lexer.Token     // Buffer for lookahead tokens.
	ctx              *ast.ASTContext   // Central registry for AST nodes and string pooling.
	errors           []ParserError     // Collected diagnostic errors.
	prevTokenEndLine uint32            // End line of the most recently consumed token.
	prevTokenEndCol  uint32            // End column of the most recently consumed token.
	curLineStartCol  uint32            // Column of the first token on the current line.
	curLine          uint32            // Current line number being processed.
	lineStartCols    map[uint32]uint32 // Map of line number to its first token's column.
}

// Parse executes the parsing logic to construct a ProgramNode.
// It processes top-level declarations (imports, resources, steps, handlers, and workflows)
// until it reaches the end of the token stream (EOF).
func (p *Parser) Parse() ast.ProgramNode {
	program := ast.ProgramNode{
		ImportRefsStart:   uint32(len(p.ctx.ImportRefs)),
		ResourceRefsStart: uint32(len(p.ctx.ResourceRefs)),
		StepRefsStart:     uint32(len(p.ctx.StepRefs)),
		HandlerRefsStart:  uint32(len(p.ctx.HandlerRefs)),
		WorkflowRefsStart: uint32(len(p.ctx.WorkflowRefs)),
		CommentRefsStart:  uint32(len(p.ctx.CommentRefs)),
	}

	for !p.curTokenIs(lexer.EOF) {
		// Skip whitespace-related tokens that are not meaningful at the top level.
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}

		switch p.curToken.Type {
		case lexer.BLOCK_COMMENT:
			p.ctx.AddCommentRef(p.ctx.AddCommentNode(ast.CommentNode{
				ValueRef: p.ctx.AddString(p.curToken.Literal),
			}))
			p.nextToken()
		case lexer.IMPORT:
			p.ctx.AddImportRef(p.parseImport())
		case lexer.RESOURCE:
			p.ctx.AddResourceRef(p.parseResource())
		case lexer.STEP:
			p.ctx.AddStepRef(p.parseStepBinding())
		case lexer.HANDLER:
			p.ctx.AddHandlerRef(p.parseHandler())
		case lexer.WORKFLOW:
			p.ctx.AddWorkflowRef(p.parseWorkflow())
		default:
			p.curError("unexpected token at top level")
			p.synchronizeTopLevel()
		}
	}

	program.ImportRefsEnd = uint32(len(p.ctx.ImportRefs))
	program.ResourceRefsEnd = uint32(len(p.ctx.ResourceRefs))
	program.StepRefsEnd = uint32(len(p.ctx.StepRefs))
	program.HandlerRefsEnd = uint32(len(p.ctx.HandlerRefs))
	program.WorkflowRefsEnd = uint32(len(p.ctx.WorkflowRefs))
	program.CommentRefsEnd = uint32(len(p.ctx.CommentRefs))

	return program
}

// expectPeek advances to the next token if it matches the expected type, otherwise records an error.
func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

// expectCur validates that the current token matches the expected type.
func (p *Parser) expectCur(t lexer.TokenType) bool {
	if p.curTokenIs(t) {
		return true
	}
	p.curError("expected " + string(t))
	return false
}

// Errors returns the list of diagnostic errors encountered during parsing.
func (p *Parser) Errors() []ParserError {
	return p.errors
}

// peekError records an error when the next token does not match the expected type.
func (p *Parser) peekError(t lexer.TokenType) {
	pk := p.peek(0)
	p.errors = append(p.errors, ParserError{
		Message: "expected " + string(t) + " but got " + string(pk.Type) + " (" + pk.Literal + ")",
		Range: ast.Range{
			Start: ast.Position{Line: uint32(pk.Line), Col: uint32(pk.Column)},
			End:   ast.Position{Line: uint32(pk.EndLine), Col: uint32(pk.EndColumn)},
		},
	})
}

// curError records a diagnostic error at the current token's position.
func (p *Parser) curError(msg string) {
	p.errors = append(p.errors, ParserError{
		Message: msg + " (got " + string(p.curToken.Type) + ": " + p.curToken.Literal + ")",
		Range: ast.Range{
			Start: ast.Position{Line: uint32(p.curToken.Line), Col: uint32(p.curToken.Column)},
			End:   ast.Position{Line: uint32(p.curToken.EndLine), Col: uint32(p.curToken.EndColumn)},
		},
	})
}

// getPos returns the start position of the current token.
func (p *Parser) getPos() ast.Position {
	return ast.Position{
		Line: uint32(p.curToken.Line),
		Col:  uint32(p.curToken.Column),
	}
}

// getEndPos returns the end position of the current token.
func (p *Parser) getEndPos() ast.Position {
	return ast.Position{
		Line: uint32(p.curToken.EndLine),
		Col:  uint32(p.curToken.EndColumn),
	}
}

// getRange calculates the source range from a start position to the end of the current token.
func (p *Parser) getRange(start ast.Position) ast.Range {
	return ast.Range{
		Start: start,
		End: ast.Position{
			Line: p.prevTokenEndLine,
			Col:  p.prevTokenEndCol,
		},
	}
}

// isTopLevelKeyword identifies tokens that mark the start of top-level declarations.
func (p *Parser) isTopLevelKeyword(t lexer.TokenType) bool {
	switch t {
	case lexer.IMPORT, lexer.RESOURCE, lexer.STEP, lexer.HANDLER, lexer.WORKFLOW:
		return true
	default:
		return false
	}
}

// synchronizeTopLevel skips tokens until it finds a top-level keyword or EOF to recover from errors.
func (p *Parser) synchronizeTopLevel() {
	p.nextToken()
	for !p.curTokenIs(lexer.EOF) {
		if p.isTopLevelKeyword(p.curToken.Type) {
			return
		}
		p.nextToken()
	}
}

// parseImport handles 'import' declarations for external Heddle modules.
func (p *Parser) parseImport() ast.NodeRef {
	node := ast.ImportNode{}
	if p.expectPeek(lexer.STRING_LIT) {
		node.PathRef = p.ctx.AddString(p.curToken.Literal)
	}
	var aliasRange ast.Range
	if p.peekTokenIs(lexer.IDENTIFIER) {
		p.nextToken()
		aliasRange = ast.Range{
			Start: ast.Position{Line: uint32(p.curToken.Line), Col: uint32(p.curToken.Column)},
			End:   ast.Position{Line: uint32(p.curToken.EndLine), Col: uint32(p.curToken.EndColumn)},
		}
		node.AliasRef = p.ctx.AddString(p.curToken.Literal)
	}
	p.nextToken()
	ref := p.ctx.AddImportNode(node)
	if node.AliasRef.Start != node.AliasRef.End {
		p.ctx.SetImportRange(ref, aliasRange)
	}
	return ref
}

// parseResource handles 'resource' declarations for stateful external dependencies (e.g., databases).
func (p *Parser) parseResource() ast.NodeRef {
	start := p.getPos()
	node := ast.ResourceBindingNode{}
	if !p.expectPeek(lexer.IDENTIFIER) {
		p.nextToken()
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.ASSIGN) {
		p.nextToken()
		return 0
	}
	p.nextToken()
	node.FunctionRef = p.parseFunctionRef()
	ref := p.ctx.AddResourceNode(node)
	p.ctx.SetResourceRange(ref, p.getRange(start))
	return ref
}

// parseStepBinding handles 'step' declarations that bind imperative code to a Heddle identifier.
func (p *Parser) parseStepBinding() ast.NodeRef {
	start := p.getPos()
	node := ast.StepBindingNode{}
	if !p.expectPeek(lexer.IDENTIFIER) {
		p.nextToken()
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.ASSIGN) {
		p.nextToken()
		return 0
	}
	p.nextToken()
	node.FunctionRef = p.parseFunctionRef()
	ref := p.ctx.AddStepBindingNode(node)
	p.ctx.SetStepRange(ref, p.getRange(start))
	return ref
}

// parseFunctionRef parses a reference to a function, potentially including module, config, and resource mappings.
func (p *Parser) parseFunctionRef() ast.NodeRef {
	fr := ast.FunctionRefNode{}

	// Optional resource_ref: [ resource_ref ]
	if p.curTokenIs(lexer.LANGLE) {
		fr.ResourcesRefRef = p.parseResourceRef()
		// After parseResourceRef, p.curToken is the token AFTER the closing '>'
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.curError("newline not allowed after resource ref")
		}
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
		}
		if p.curToken.Line == int(p.prevTokenEndLine) && p.curToken.Column == int(p.prevTokenEndCol) {
			p.curError("missing space after resource ref")
		}
	}

	// [ IDENTIFIER "." ] IDENTIFIER
	if p.curTokenIs(lexer.IDENTIFIER) {
		ident1 := p.curToken.Literal
		if p.peekTokenIs(lexer.DOT) {
			p.nextToken() // Skip '.'
			if p.expectPeek(lexer.IDENTIFIER) {
				fr.ModuleRef = p.ctx.AddString(ident1)
				fr.NameRef = p.ctx.AddString(p.curToken.Literal)
				p.nextToken()
			}
		} else {
			fr.NameRef = p.ctx.AddString(ident1)
			p.nextToken()
		}
	} else {
		p.curError("expected function identifier")
	}

	// [ function_config ]
	if p.curTokenIs(lexer.LBRACE) {
		fr.ConfigRef = p.parseDict(true)
	}

	return p.ctx.AddFunctionRefNode(fr)
}

// parseResourceRef parses resource mapping blocks defined within angle brackets '<...>'.
func (p *Parser) parseResourceRef() ast.NodeRef {
	node := ast.ResourceRefNode{
		MappingsRefStart: uint32(len(p.ctx.MappingRefs)),
	}
	p.nextToken() // Skip '<'
	for !p.curTokenIs(lexer.RANGLE) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.IDENTIFIER) {
			mapping := ast.ResourceMappingNode{
				KeyRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.ASSIGN) {
				if p.expectPeek(lexer.IDENTIFIER) {
					mapping.ValueRef = p.ctx.AddString(p.curToken.Literal)
					p.nextToken()
				} else {
					p.nextToken()
				}
			} else {
				p.nextToken()
			}
			p.ctx.AddMappingRef(p.ctx.AddResourceMappingNode(mapping))
		} else if p.curTokenIs(lexer.COMMA) {
			p.nextToken()
		} else {
			p.curError("unexpected token in resource mapping")
			p.nextToken()
		}
	}
	if p.curTokenIs(lexer.RANGLE) {
		p.nextToken()
	}
	node.MappingsRefEnd = uint32(len(p.ctx.MappingRefs))
	return p.ctx.AddResourceRefNode(node)
}

// parseHandler handles 'handler' blocks used for reusable error recovery logic.
func (p *Parser) parseHandler() ast.NodeRef {
	start := p.getPos()
	node := ast.HandlerNode{
		HandlerStatementRefsStart: uint32(len(p.ctx.HandlerStatementRefs)),
	}
	if !p.expectPeek(lexer.IDENTIFIER) {
		p.nextToken()
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // Move to '?'
		p.curError("handlers cannot have error traps")
		if p.peekTokenIs(lexer.IDENTIFIER) {
			p.nextToken() // Move past trap name
		}
	}
	if !p.expectPeek(lexer.LBRACE) {
		p.nextToken()
		return 0
	}
	p.nextToken() // Move past '{'

	for !p.curTokenIs(lexer.RBRACE) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			if p.isNextPipe() {
				// Let parseHandlerStatement -> parsePipeChainFromPipe handle it.
				p.ctx.AddHandlerStatementRef(p.parseHandlerStatement())
				continue
			}
			p.nextToken()
			continue
		}
		p.ctx.AddHandlerStatementRef(p.parseHandlerStatement())
	}

	if p.curTokenIs(lexer.RBRACE) {
		p.nextToken()
	}

	node.HandlerStatementRefsEnd = uint32(len(p.ctx.HandlerStatementRefs))
	ref := p.ctx.AddHandlerNode(node)
	p.ctx.SetHandlerRange(ref, p.getRange(start))
	return ref
}

// parseHandlerStatement parses a single execution step within a handler block.
func (p *Parser) parseHandlerStatement() ast.NodeRef {
	hs := ast.HandlerStatementNode{}
	if p.curTokenIs(lexer.ASTERISK) {
		hs.IsCatchAll = true
		p.nextToken() // Skip '*'
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) {
			if p.isNextPipe() {
				break
			}
			p.nextToken()
		}
	}
	if p.curTokenIs(lexer.PIPE) || p.isNextPipe() {
		chainRef := p.parsePipeChainFromPipe()
		ps := ast.PipelineStatementNode{ExprRef: chainRef}
		// Handle optional assignment
		for {
			if p.curTokenIs(lexer.RANGLE) {
				p.nextToken()
				for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
					p.nextToken()
				}
				if p.curTokenIs(lexer.IDENTIFIER) {
					ps.AssignmentRef = p.ctx.AddString(p.curToken.Literal)
					identRange := ast.Range{
						Start: ast.Position{Line: uint32(p.curToken.Line), Col: uint32(p.curToken.Column)},
						End:   ast.Position{Line: uint32(p.curToken.EndLine), Col: uint32(p.curToken.EndColumn)},
					}
					p.nextToken()
					ref := p.ctx.AddPipelineStatementNode(ps)
					p.ctx.SetAssignmentRange(ref, identRange)
					hs.StmtRef = ref
				} else {
					p.curError("expected identifier after '>'")
					hs.StmtRef = p.ctx.AddPipelineStatementNode(ps)
				}
				break
			}

			if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
				isNextRangle := false
				for n := 0; ; n++ {
					tok := p.peek(n)
					if tok.Type == lexer.NEWLINE || tok.Type == lexer.INDENT || tok.Type == lexer.DEDENT {
						continue
					}
					if tok.Type == lexer.RANGLE {
						isNextRangle = true
					}
					break
				}
				if isNextRangle {
					p.nextToken()
					continue
				}
			}
			hs.StmtRef = p.ctx.AddPipelineStatementNode(ps)
			break
		}
	} else {
		hs.StmtRef = p.parsePipelineStatement()
	}
	return p.ctx.AddHandlerStatementNode(hs)
}

// parseWorkflow handles 'workflow' blocks, defining the core DAG orchestration.
func (p *Parser) parseWorkflow() ast.NodeRef {
	start := p.getPos()
	node := ast.WorkflowNode{
		StatementRefsStart: uint32(len(p.ctx.StatementRefs)),
	}
	if !p.expectPeek(lexer.IDENTIFIER) {
		p.nextToken()
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // Skip '?'
		if p.expectPeek(lexer.IDENTIFIER) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	} else {
		p.nextToken()
	}
	p.expectCur(lexer.LBRACE)
	p.nextToken()

	for !p.curTokenIs(lexer.RBRACE) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}
		p.ctx.AddStatementRef(p.parsePipelineStatement())
	}

	if p.curTokenIs(lexer.RBRACE) {
		p.nextToken()
	}

	node.StatementRefsEnd = uint32(len(p.ctx.StatementRefs))
	ref := p.ctx.AddWorkflowNode(node)
	p.ctx.SetWorkflowRange(ref, p.getRange(start))
	return ref
}

// parsePipelineStatement parses a single statement in a workflow, which can be a dataframe or a pipe chain.
func (p *Parser) parsePipelineStatement() ast.NodeRef {
	ps := ast.PipelineStatementNode{}
	if p.curTokenIs(lexer.LBRACKET) {
		dfRef := p.parseDataframe()
		// If followed by a pipe, it's a pipe chain starting with a dataframe.
		if p.curTokenIs(lexer.PIPE) || p.isNextPipe() {
			ps.ExprRef = p.parsePipeChainFromExpr(dfRef)
		} else {
			ps.ExprRef = p.parsePipeChainFromExpr(dfRef)
		}
	} else {
		ps.ExprRef = p.parsePipeChain()
	}

	for {
		if p.curTokenIs(lexer.RANGLE) {
			p.nextToken() // Move past '>'
			// Skip any whitespace/indentation before identifier
			for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
				p.nextToken()
			}
			if p.curTokenIs(lexer.IDENTIFIER) {
				ps.AssignmentRef = p.ctx.AddString(p.curToken.Literal)
				identRange := ast.Range{
					Start: ast.Position{Line: uint32(p.curToken.Line), Col: uint32(p.curToken.Column)},
					End:   ast.Position{Line: uint32(p.curToken.EndLine), Col: uint32(p.curToken.EndColumn)},
				}
				p.nextToken()
				ref := p.ctx.AddPipelineStatementNode(ps)
				p.ctx.SetAssignmentRange(ref, identRange)
				return ref
			} else {
				p.curError("expected identifier after '>'")
			}
			break
		}

		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			// Check if RANGLE is coming up after some more whitespace/indentation
			isNextRangle := false
			for n := 0; ; n++ {
				tok := p.peek(n)
				if tok.Type == lexer.NEWLINE || tok.Type == lexer.INDENT || tok.Type == lexer.DEDENT {
					continue
				}
				if tok.Type == lexer.RANGLE {
					isNextRangle = true
				}
				break
			}

			if isNextRangle {
				p.nextToken()
				continue
			}
		}
		break
	}

	return p.ctx.AddPipelineStatementNode(ps)
}

// parsePipeChain parses a series of function calls linked by pipes '|'.
func (p *Parser) parsePipeChain() ast.NodeRef {
	node := ast.PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}
	p.ctx.AddCallRef(p.parseCall())
	indentDepth := 0
	for {
		hadWhitespace := false
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			// Handle multi-line pipe chains.
			// If we see a PIPE coming up after some whitespace/indentation, consume it.
			if p.isNextPipe() {
				if p.curTokenIs(lexer.INDENT) {
					indentDepth++
				} else if p.curTokenIs(lexer.DEDENT) {
					indentDepth--
				}
				p.nextToken()
				hadWhitespace = true
				continue
			}
			break
		}

		if p.curTokenIs(lexer.PIPE) {
			if !hadWhitespace {
				p.curError("pipe cannot follow a call on the same line; use a newline")
			}
			p.nextToken() // Move past '|'
			if p.curToken.Line == int(p.prevTokenEndLine) && p.curToken.Column == int(p.prevTokenEndCol) {
				p.curError("missing space after pipe")
			}
			p.ctx.AddCallRef(p.parseCall())
			continue
		}
		break
	}

	// Consume matching DEDENTs that were caused by internal indentation of the pipe chain.
	for indentDepth > 0 && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.DEDENT) {
			indentDepth--
			p.nextToken()
			continue
		}
		break
	}

	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

// parsePipeChainFromPipe parses a pipe chain starting from a pipe token, often used for implicit input passing.
func (p *Parser) parsePipeChainFromPipe() ast.NodeRef {
	node := ast.PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}
	// Inject an implicit initial call to represent the input data.
	p.ctx.AddCallRef(p.ctx.AddCallNode(ast.CallNode{}))
	indentDepth := 0
	for {
		hadWhitespace := false
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			if p.isNextPipe() {
				if p.curTokenIs(lexer.INDENT) {
					indentDepth++
				} else if p.curTokenIs(lexer.DEDENT) {
					indentDepth--
				}
				p.nextToken()
				hadWhitespace = true
				continue
			}
			break
		}

		if p.curTokenIs(lexer.PIPE) {
			if !hadWhitespace {
				p.curError("pipe cannot follow a call on the same line; use a newline")
			}
			p.nextToken() // Move past '|'
			p.ctx.AddCallRef(p.parseCall())
			continue
		}
		break
	}

	// Consume matching DEDENTs.
	for indentDepth > 0 && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.DEDENT) {
			indentDepth--
			p.nextToken()
			continue
		}
		break
	}

	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

func (p *Parser) parseCall() ast.NodeRef {
	start := p.getPos()
	node := ast.CallNode{}

	if p.curTokenIs(lexer.PRQL_BLOCK) {
		// query_block (anonymous_step_call)
		node.QueryRef = p.ctx.AddString(p.curToken.Literal)
		node.IsPrql = true
		p.nextToken()
	} else if p.curTokenIs(lexer.LANGLE) {
		// function_ref (anonymous_step_call) starting with resource_ref
		node.FunctionRef = p.parseFunctionRef()
	} else if p.curTokenIs(lexer.IDENTIFIER) {
		// Could be a standard step_call or an anonymous function_ref.
		// Look ahead to determine if it's a function_ref (module.fn or fn {config}).
		if p.peekTokenIs(lexer.DOT) || p.peekTokenIs(lexer.LBRACE) {
			node.FunctionRef = p.parseFunctionRef()
		} else {
			// standard step_call
			node.NameRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	} else {
		p.curError("expected call (IDENT, resource_ref, or PRQL block)")
		// Synchronize: skip until NEWLINE or PIPE to recover parsing.
		for !p.curTokenIs(lexer.EOF) && !p.curTokenIs(lexer.NEWLINE) && !p.curTokenIs(lexer.PIPE) {
			p.nextToken()
		}
	}

	// Optional trap handler: ?handler
	if p.curTokenIs(lexer.QUESTION) {
		if p.expectPeek(lexer.IDENTIFIER) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	}
	ref := p.ctx.AddCallNode(node)
	p.ctx.SetCallRange(ref, p.getRange(start))
	return ref
}

// parseDataframe parses a list of dictionaries representing a tabular data structure.
func (p *Parser) parseDataframe() ast.NodeRef {
	openerLine := uint32(p.curToken.Line)
	openerColumn := uint32(p.curToken.Column)
	openerLineStartCol := p.lineStartCols[openerLine]
	node := ast.DataframeNode{
		DictRefsStart: uint32(len(p.ctx.DictRefs)),
	}
	p.nextToken() // Skip '['

	isMultiline := false
	if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.DEDENT) {
		isMultiline = true
		if p.curTokenIs(lexer.DEDENT) {
			p.curError("expected indentation after newline in dataframe")
		} else {
			p.nextToken() // Skip NEWLINE
			if p.curTokenIs(lexer.INDENT) {
				p.nextToken() // Skip INDENT
			} else if !p.curTokenIs(lexer.RBRACKET) {
				p.curError("expected indentation after newline in dataframe")
			}
		}
	}

	lastContentEndLine := openerLine
	for !p.curTokenIs(lexer.RBRACKET) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			if !isMultiline {
				p.curError("newline not allowed in single-line dataframe")
			}
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.COMMA) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.LBRACE) {
			p.ctx.AddDictRef(p.parseDict(false))
			lastContentEndLine = p.prevTokenEndLine
			continue
		}
		p.curError("unexpected token in dataframe")
		p.nextToken()
	}

	if isMultiline {
		if uint32(p.curToken.Line) == lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing bracket must be on a new line in multiline dataframe")
		}
		col := uint32(p.curToken.Column)
		if (col < openerLineStartCol || col > openerColumn) && !p.curTokenIs(lexer.EOF) {
			p.curError("misaligned closer")
		}
	} else {
		if uint32(p.curToken.Line) != openerLine && uint32(p.curToken.Line) != lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing bracket must be on the same line in single-line dataframe")
		}
	}

	if p.curTokenIs(lexer.RBRACKET) {
		p.nextToken()
	}
	node.DictRefsEnd = uint32(len(p.ctx.DictRefs))
	return p.ctx.AddDataframeNode(node)
}

// parseDict parses a key-value dictionary block.
func (p *Parser) parseDict(allowNested bool) ast.NodeRef {
	start := p.getPos()
	openerLine := uint32(p.curToken.Line)
	openerColumn := uint32(p.curToken.Column)
	openerLineStartCol := p.lineStartCols[openerLine]
	var pairs []ast.NodeRef
	p.expectCur(lexer.LBRACE)
	p.nextToken()

	// Handle multiline indentation enforcement
	isMultiline := false
	if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.DEDENT) {
		isMultiline = true
		if p.curTokenIs(lexer.DEDENT) {
			p.curError("expected indentation after newline in dict")
		} else {
			p.nextToken() // Skip NEWLINE
			if p.curTokenIs(lexer.INDENT) {
				p.nextToken() // Skip INDENT
			} else if !p.curTokenIs(lexer.RBRACE) {
				p.curError("expected indentation after newline in dict")
			}
		}
	}

	lastContentEndLine := openerLine
	for !p.curTokenIs(lexer.RBRACE) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			if !isMultiline {
				p.curError("newline not allowed in single-line dict")
			}
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.IDENTIFIER) || p.curTokenIs(lexer.STRING_LIT) {
			pair := ast.PairNode{
				KeyRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.COLON) {
				p.nextToken()
				pair.ValueRef = p.parseLiteral(allowNested)
			} else {
				pair.ValueRef = 0
				p.nextToken()
			}
			lastContentEndLine = p.prevTokenEndLine
			pairs = append(pairs, p.ctx.AddPairNode(pair))
			if p.curTokenIs(lexer.COMMA) {
				p.nextToken()
			} else if !isMultiline && !p.curTokenIs(lexer.RBRACE) {
				p.curError("pairs in single-line dict must be comma-separated")
			}
			continue
		}
		p.curError("unexpected token in dict")
		p.nextToken()
	}

	if isMultiline {
		if uint32(p.curToken.Line) == lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing brace must be on a new line in multiline dict")
		}
		col := uint32(p.curToken.Column)
		if (col < openerLineStartCol || col > openerColumn) && !p.curTokenIs(lexer.EOF) {
			p.curError("misaligned closer")
		}
	} else {
		if uint32(p.curToken.Line) != openerLine && uint32(p.curToken.Line) != lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing brace must be on the same line in single-line dict")
		}
		if len(pairs) == 0 && uint32(p.curToken.Column) == openerColumn+1 {
			p.curError("empty single-line dict must have at least one space or be multiline")
		}
	}

	var end ast.Position
	if p.curTokenIs(lexer.RBRACE) {
		end = p.getEndPos()
		p.nextToken()
	} else {
		end = p.getEndPos()
	}

	node := ast.DictNode{
		PairRefsStart: uint32(len(p.ctx.PairRefs)),
	}
	for _, ref := range pairs {
		p.ctx.AddPairRef(ref)
	}
	node.PairRefsEnd = uint32(len(p.ctx.PairRefs))
	ref := p.ctx.AddDictNode(node)
	p.ctx.SetDictRange(ref, ast.Range{Start: start, End: end})
	return ref
}

// parseList parses a square-bracketed list of literal values.
func (p *Parser) parseList(allowNested bool) ast.NodeRef {
	openerLine := uint32(p.curToken.Line)
	openerColumn := uint32(p.curToken.Column)
	openerLineStartCol := p.lineStartCols[openerLine]
	var literals []ast.NodeRef
	p.nextToken() // Skip '['

	isMultiline := false
	if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.DEDENT) {
		isMultiline = true
		if p.curTokenIs(lexer.DEDENT) {
			p.curError("expected indentation after newline in list")
		} else {
			p.nextToken() // Skip NEWLINE
			if !p.curTokenIs(lexer.INDENT) {
				p.curError("expected indentation after newline in list")
			} else {
				p.nextToken() // Skip INDENT
			}
		}
	}

	lastContentEndLine := openerLine
	for !p.curTokenIs(lexer.RBRACKET) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			if !isMultiline {
				p.curError("newline not allowed in single-line list")
			}
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.COMMA) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}
		literals = append(literals, p.parseLiteral(allowNested))
		lastContentEndLine = p.prevTokenEndLine
	}

	if isMultiline {
		if uint32(p.curToken.Line) == lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing bracket must be on a new line in multiline list")
		}
		col := uint32(p.curToken.Column)
		if (col < openerLineStartCol || col > openerColumn) && !p.curTokenIs(lexer.EOF) {
			p.curError("misaligned closer")
		}
	} else {
		if uint32(p.curToken.Line) != openerLine && uint32(p.curToken.Line) != lastContentEndLine && !p.curTokenIs(lexer.EOF) {
			p.curError("closing bracket must be on the same line in single-line list")
		}
	}

	if p.curTokenIs(lexer.RBRACKET) {
		p.nextToken()
	}

	node := ast.ListNode{
		LiteralRefsStart: uint32(len(p.ctx.LiteralRefs)),
	}
	for _, ref := range literals {
		p.ctx.AddLiteralRef(ref)
	}
	node.LiteralRefsEnd = uint32(len(p.ctx.LiteralRefs))
	return p.ctx.AddListNode(node)
}

// parseLiteral parses primitive values, strings, numbers, booleans, nulls, or nested structures.
func (p *Parser) parseLiteral(allowNested bool) ast.NodeRef {
	node := ast.LiteralNode{}
	switch p.curToken.Type {
	case lexer.STRING_LIT:
		node.Type = ast.LiteralString
		node.ValueRef = p.ctx.AddString(p.curToken.Literal)
		p.nextToken()
	case lexer.INT:
		node.Type = ast.LiteralInt
		node.ValueRef = p.ctx.AddString(p.curToken.Literal)
		p.nextToken()
	case lexer.FLOAT:
		node.Type = ast.LiteralFloat
		node.ValueRef = p.ctx.AddString(p.curToken.Literal)
		p.nextToken()
	case lexer.TRUE:
		node.Type = ast.LiteralBool
		node.ValueRef = p.ctx.AddString("true")
		p.nextToken()
	case lexer.FALSE:
		node.Type = ast.LiteralBool
		node.ValueRef = p.ctx.AddString("false")
		p.nextToken()
	case lexer.NULL:
		node.Type = ast.LiteralNull
		node.ValueRef = p.ctx.AddString("null")
		p.nextToken()
	case lexer.LBRACE:
		if !allowNested {
			p.curError("nested dictionaries are not allowed in this context")
		}
		node.Type = ast.LiteralDict
		node.Ref = p.parseDict(allowNested)
	case lexer.LBRACKET:
		if !allowNested {
			p.curError("nested lists are not allowed in this context")
		}
		node.Type = ast.LiteralList
		node.Ref = p.parseList(allowNested)
	default:
		p.curError("expected literal")
		p.nextToken()
	}
	return p.ctx.AddLiteralNode(node)
}

// peekTokenIs checks if the next token matches the specified type.
func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peek(0).Type == t
}

// curTokenIs checks if the current token matches the specified type.
func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) nextToken() {
	p.prevTokenEndLine = uint32(p.curToken.EndLine)
	p.prevTokenEndCol = uint32(p.curToken.EndColumn)
	p.curToken = p.peek(0)
	if _, ok := p.lineStartCols[uint32(p.curToken.Line)]; !ok {
		// Only track the first token that isn't a formatting token
		if p.curToken.Type != lexer.INDENT && p.curToken.Type != lexer.DEDENT && p.curToken.Type != lexer.NEWLINE {
			p.lineStartCols[uint32(p.curToken.Line)] = uint32(p.curToken.Column)
		}
	}
	// Track the column of the first token on each line
	if uint32(p.curToken.Line) > p.curLine {
		p.curLine = uint32(p.curToken.Line)
		if p.curToken.Type != lexer.INDENT && p.curToken.Type != lexer.DEDENT && p.curToken.Type != lexer.NEWLINE {
			p.curLineStartCol = uint32(p.curToken.Column)
		} else {
			p.curLineStartCol = 0 // Will be set by the next meaningful token on this line
		}
	} else if p.curLineStartCol == 0 && p.curToken.Type != lexer.INDENT && p.curToken.Type != lexer.DEDENT && p.curToken.Type != lexer.NEWLINE {
		p.curLineStartCol = uint32(p.curToken.Column)
	}
	if len(p.peekTokens) > 0 {
		p.peekTokens = p.peekTokens[1:]
	}
}

// peek returns the n-th token ahead of the current position without advancing the parser.
func (p *Parser) peek(n int) lexer.Token {
	for len(p.peekTokens) <= n {
		p.peekTokens = append(p.peekTokens, p.l.NextToken())
	}
	return p.peekTokens[n]
}

// isNextPipe checks if the next non-whitespace token is a pipe '|'.
func (p *Parser) isNextPipe() bool {
	for n := 0; ; n++ {
		tok := p.peek(n)
		if tok.Type == lexer.NEWLINE || tok.Type == lexer.INDENT || tok.Type == lexer.DEDENT {
			continue
		}
		return tok.Type == lexer.PIPE
	}
}

// parsePipeChainFromExpr constructs a pipe chain starting with an existing expression (like a dataframe).
func (p *Parser) parsePipeChainFromExpr(exprRef ast.NodeRef) ast.NodeRef {
	node := ast.PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}
	// Wrap the expression in a CallNode
	p.ctx.AddCallRef(p.ctx.AddCallNode(ast.CallNode{DataframeRef: exprRef}))

	indentDepth := 0
	for {
		hadWhitespace := false
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			if p.isNextPipe() {
				if p.curTokenIs(lexer.INDENT) {
					indentDepth++
				} else if p.curTokenIs(lexer.DEDENT) {
					indentDepth--
				}
				p.nextToken()
				hadWhitespace = true
				continue
			}
			break
		}

		if p.curTokenIs(lexer.PIPE) {
			if !hadWhitespace {
				p.curError("pipe cannot follow a call on the same line; use a newline")
			}
			p.nextToken() // Move past '|'
			p.ctx.AddCallRef(p.parseCall())
			continue
		}
		break
	}

	// Consume matching DEDENTs.
	for indentDepth > 0 && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.DEDENT) {
			indentDepth--
			p.nextToken()
			continue
		}
		break
	}

	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

// New initializes a new Parser instance with a lexer and an AST context.
func New(l *lexer.Lexer, ctx *ast.ASTContext) *Parser {
	p := &Parser{
		l:   l,
		ctx: ctx,
	}
	p.lineStartCols = make(map[uint32]uint32)
	p.nextToken()
	return p
}
