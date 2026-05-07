package parser

import (
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

type ParserError struct {
	Message string
	Line    int
	Column  int
}

type Parser struct {
	l          *lexer.Lexer
	curToken   lexer.Token
	peekTokens []lexer.Token
	ctx        *ast.ASTContext
	errors     []ParserError
}

func New(l *lexer.Lexer, ctx *ast.ASTContext) *Parser {
	p := &Parser{
		l:   l,
		ctx: ctx,
	}
	p.nextToken()
	return p
}

func (p *Parser) peek(n int) lexer.Token {
	for len(p.peekTokens) <= n {
		p.peekTokens = append(p.peekTokens, p.l.NextToken())
	}
	return p.peekTokens[n]
}

func (p *Parser) nextToken() {
	p.curToken = p.peek(0)
	if len(p.peekTokens) > 0 {
		p.peekTokens = p.peekTokens[1:]
	}
}

func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peek(0).Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) expectCur(t lexer.TokenType) bool {
	if p.curTokenIs(t) {
		return true
	}
	p.curError("expected " + string(t))
	return false
}

func (p *Parser) Errors() []ParserError {
	return p.errors
}

func (p *Parser) peekError(t lexer.TokenType) {
	pk := p.peek(0)
	p.errors = append(p.errors, ParserError{
		Message: "expected " + string(t) + " but got " + string(pk.Type) + " (" + pk.Literal + ")",
		Line:    pk.Line,
		Column:  pk.Column,
	})
}

func (p *Parser) curError(msg string) {
	p.errors = append(p.errors, ParserError{
		Message: msg + " (got " + string(p.curToken.Type) + ": " + p.curToken.Literal + ") peek(0)=" + string(p.peek(0).Type) + " peek(1)=" + string(p.peek(1).Type),
		Line:    p.curToken.Line,
		Column:  p.curToken.Column,
	})
}

func (p *Parser) getPos() ast.Position {
	return ast.Position{
		Line: uint32(p.curToken.Line),
		Col:  uint32(p.curToken.Column),
	}
}

func (p *Parser) getEndPos() ast.Position {
	return ast.Position{
		Line: uint32(p.curToken.EndLine),
		Col:  uint32(p.curToken.EndColumn),
	}
}

func (p *Parser) getRange(start ast.Position) ast.Range {
	return ast.Range{
		Start: start,
		End:   p.getEndPos(),
	}
}

func (p *Parser) Parse() ast.ProgramNode {
	program := ast.ProgramNode{
		ImportRefsStart:   uint32(len(p.ctx.ImportRefs)),
		ResourceRefsStart: uint32(len(p.ctx.ResourceRefs)),
		StepRefsStart:     uint32(len(p.ctx.StepRefs)),
		HandlerRefsStart:  uint32(len(p.ctx.HandlerRefs)),
		WorkflowRefsStart: uint32(len(p.ctx.WorkflowRefs)),
	}

	for !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}

		switch p.curToken.Type {
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

	return program
}

func (p *Parser) isTopLevelKeyword(t lexer.TokenType) bool {
	switch t {
	case lexer.IMPORT, lexer.RESOURCE, lexer.STEP, lexer.HANDLER, lexer.WORKFLOW:
		return true
	default:
		return false
	}
}

func (p *Parser) synchronizeTopLevel() {
	p.nextToken()
	for !p.curTokenIs(lexer.EOF) {
		if p.isTopLevelKeyword(p.curToken.Type) {
			return
		}
		p.nextToken()
	}
}

func (p *Parser) parseImport() ast.NodeRef {
	node := ast.ImportNode{}
	if p.expectPeek(lexer.STRING_LIT) {
		node.PathRef = p.ctx.AddString(p.curToken.Literal)
	}
	if p.expectPeek(lexer.IDENT) {
		node.AliasRef = p.ctx.AddString(p.curToken.Literal)
	}
	p.nextToken()
	return p.ctx.AddImportNode(node)
}

func (p *Parser) parseResource() ast.NodeRef {
	start := p.getPos()
	node := ast.ResourceNode{}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.ASSIGN) {
		return 0
	}
	p.nextToken()
	node.RefRef = p.parseFunctionRef()
	ref := p.ctx.AddResourceNode(node)
	p.ctx.SetResourceRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseStepBinding() ast.NodeRef {
	start := p.getPos()
	node := ast.StepBindingNode{}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.ASSIGN) {
		return 0
	}
	p.nextToken()
	node.RefRef = p.parseFunctionRef()
	ref := p.ctx.AddStepBindingNode(node)
	p.ctx.SetStepRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseFunctionRef() ast.NodeRef {
	fr := ast.FunctionRefNode{}
	ident1 := p.curToken.Literal
	if p.peekTokenIs(lexer.DOT) {
		p.nextToken() // '.'
		if p.expectPeek(lexer.IDENT) {
			fr.ModuleRef = p.ctx.AddString(ident1)
			fr.NameRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	} else {
		fr.NameRef = p.ctx.AddString(ident1)
		p.nextToken()
	}

	if p.curTokenIs(lexer.LANGLE) {
		fr.ResourcesRef = p.parseResourceRef()
	}

	if p.curTokenIs(lexer.LBRACE) {
		fr.ConfigRef = p.parseDict()
	}

	return p.ctx.AddFunctionRefNode(fr)
}

func (p *Parser) parseResourceRef() ast.NodeRef {
	node := ast.ResourceRefNode{
		MappingsRefStart: uint32(len(p.ctx.MappingRefs)),
	}
	p.nextToken()
	for !p.curTokenIs(lexer.RANGLE) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.IDENT) {
			mapping := ast.ResourceMappingNode{
				KeyRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.ASSIGN) {
				if p.expectPeek(lexer.IDENT) {
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

func (p *Parser) parseHandler() ast.NodeRef {
	start := p.getPos()
	node := ast.HandlerNode{
		HandlerStatementRefsStart: uint32(len(p.ctx.HandlerStatementRefs)),
	}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	p.expectPeek(lexer.LBRACE)
	p.expectPeek(lexer.NEWLINE)
	p.expectPeek(lexer.INDENT)
	p.nextToken()

	for !p.curTokenIs(lexer.DEDENT) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		p.ctx.AddHandlerStatementRef(p.parseHandlerStatement())
	}

	for p.curTokenIs(lexer.DEDENT) || p.curTokenIs(lexer.NEWLINE) { p.nextToken() }
	if p.curTokenIs(lexer.RBRACE) { p.nextToken() }

	node.HandlerStatementRefsEnd = uint32(len(p.ctx.HandlerStatementRefs))
	ref := p.ctx.AddHandlerNode(node)
	p.ctx.SetHandlerRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseHandlerStatement() ast.NodeRef {
	hs := ast.HandlerStatementNode{}
	if p.curTokenIs(lexer.ASTERISK) {
		hs.IsCatchAll = true
		p.nextToken()
		for p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) {
			p.nextToken()
		}
	}
	if p.curTokenIs(lexer.PIPE) {
		chainRef := p.parsePipeChainFromPipe()
		hs.StmtRef = p.ctx.AddPipelineStatementNode(ast.PipelineStatementNode{ExprRef: chainRef})
	} else {
		hs.StmtRef = p.parsePipelineStatement()
	}
	return p.ctx.AddHandlerStatementNode(hs)
}

func (p *Parser) parseWorkflow() ast.NodeRef {
	start := p.getPos()
	node := ast.WorkflowNode{
		StatementRefsStart: uint32(len(p.ctx.StatementRefs)),
	}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken()
		if p.expectPeek(lexer.IDENT) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	} else {
		p.nextToken()
	}
	p.expectCur(lexer.LBRACE)
	p.expectPeek(lexer.NEWLINE)
	p.expectPeek(lexer.INDENT)
	p.nextToken()

	for !p.curTokenIs(lexer.DEDENT) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		p.ctx.AddStatementRef(p.parsePipelineStatement())
	}

	for p.curTokenIs(lexer.DEDENT) || p.curTokenIs(lexer.NEWLINE) { p.nextToken() }
	if p.curTokenIs(lexer.RBRACE) { p.nextToken() }

	node.StatementRefsEnd = uint32(len(p.ctx.StatementRefs))
	ref := p.ctx.AddWorkflowNode(node)
	p.ctx.SetWorkflowRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parsePipelineStatement() ast.NodeRef {
	ps := ast.PipelineStatementNode{}
	if p.curTokenIs(lexer.LBRACKET) {
		ps.ExprRef = p.parseDataframe()
	} else {
		ps.ExprRef = p.parsePipeChain()
	}

	for p.curTokenIs(lexer.NEWLINE) {
		if p.peekTokenIs(lexer.RANGLE) {
			p.nextToken() // move to '>'
			p.nextToken() // move to assignment IDENT
			if p.curTokenIs(lexer.IDENT) {
				ps.AssignmentRef = p.ctx.AddString(p.curToken.Literal)
				p.nextToken()
			}
			break
		} else if p.peekTokenIs(lexer.NEWLINE) {
			p.nextToken()
		} else {
			break
		}
	}

	return p.ctx.AddPipelineStatementNode(ps)
}

func (p *Parser) parsePipeChain() ast.NodeRef {
	node := ast.PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}
	p.ctx.AddCallRef(p.parseCall())
	for {
		if p.curTokenIs(lexer.NEWLINE) {
			if p.peekTokenIs(lexer.INDENT) && p.peek(1).Type == lexer.PIPE {
				p.nextToken() // NEWLINE
				p.nextToken() // INDENT
				p.nextToken() // PIPE
				p.ctx.AddCallRef(p.parseCall())
				continue
			}
			if p.peekTokenIs(lexer.PIPE) {
				p.nextToken() // NEWLINE
				p.nextToken() // PIPE
				p.ctx.AddCallRef(p.parseCall())
				continue
			}
		}
		break
	}
	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

func (p *Parser) parsePipeChainFromPipe() ast.NodeRef {
	node := ast.PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}
	// First call is implicit input
	p.ctx.AddCallRef(p.ctx.AddCallNode(ast.CallNode{}))
	for {
		if p.curTokenIs(lexer.PIPE) {
			p.nextToken() // first token of call
			p.ctx.AddCallRef(p.parseCall())
		} else {
			break
		}

		if p.curTokenIs(lexer.NEWLINE) {
			if p.peekTokenIs(lexer.INDENT) && p.peek(1).Type == lexer.PIPE {
				p.nextToken() // NEWLINE
				p.nextToken() // INDENT
				p.nextToken() // PIPE
				// p.nextToken() removed - parseCall should handle the start token
				continue
			} else if p.peekTokenIs(lexer.PIPE) {
				p.nextToken() // NEWLINE
				p.nextToken() // PIPE
				// p.nextToken() removed
				continue
			}
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
		node.QueryRef = p.ctx.AddString(p.curToken.Literal)
		node.IsPrql = true
		p.nextToken()
	} else if p.curTokenIs(lexer.IDENT) {
		ident1 := p.curToken.Literal
		if p.peekTokenIs(lexer.DOT) {
			p.nextToken() // DOT
			if p.peekTokenIs(lexer.IDENT) {
				p.nextToken()
				node.NameRef = p.ctx.AddString(ident1 + "." + p.curToken.Literal)
				p.nextToken()
			}
		} else {
			node.NameRef = p.ctx.AddString(ident1)
			p.nextToken()
		}
	} else {
		p.curError("expected call (IDENT or PRQL block)")
		// Synchronize: skip until NEWLINE or PIPE
		for !p.curTokenIs(lexer.EOF) && !p.curTokenIs(lexer.NEWLINE) && !p.curTokenIs(lexer.PIPE) {
			p.nextToken()
		}
	}

	if p.curTokenIs(lexer.QUESTION) {
		if p.expectPeek(lexer.IDENT) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
			p.nextToken()
		}
	}
	ref := p.ctx.AddCallNode(node)
	p.ctx.SetCallRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseDataframe() ast.NodeRef {
	node := ast.DataframeNode{
		DictRefsStart: uint32(len(p.ctx.DictRefs)),
	}
	p.nextToken() // skip '['
	for !p.curTokenIs(lexer.RBRACKET) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.COMMA) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.LBRACE) {
			p.ctx.AddDictRef(p.parseDict())
			continue
		}
		p.curError("unexpected token in dataframe")
		p.nextToken()
	}
	if p.curTokenIs(lexer.RBRACKET) { p.nextToken() }
	node.DictRefsEnd = uint32(len(p.ctx.DictRefs))
	return p.ctx.AddDataframeNode(node)
}

func (p *Parser) parseDict() ast.NodeRef {
	node := ast.DictNode{
		PairRefsStart: uint32(len(p.ctx.PairRefs)),
	}
	p.expectCur(lexer.LBRACE)
	p.expectPeek(lexer.NEWLINE)
	p.expectPeek(lexer.INDENT)
	p.nextToken()

	for !p.curTokenIs(lexer.DEDENT) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}
		if p.curTokenIs(lexer.IDENT) || p.curTokenIs(lexer.STRING_LIT) {
			pair := ast.PairNode{
				KeyRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.COLON) {
				p.nextToken()
				pair.ValueRef = p.parseLiteral()
			} else {
				pair.ValueRef = 0
				p.nextToken()
			}
			p.ctx.AddPairRef(p.ctx.AddPairNode(pair))
			if p.curTokenIs(lexer.COMMA) {
				p.nextToken()
			}
			continue
		}
		p.curError("unexpected token in dict")
		p.nextToken()
	}

	for p.curTokenIs(lexer.DEDENT) || p.curTokenIs(lexer.NEWLINE) { p.nextToken() }
	if p.curTokenIs(lexer.RBRACE) { p.nextToken() }

	node.PairRefsEnd = uint32(len(p.ctx.PairRefs))
	return p.ctx.AddDictNode(node)
}

func (p *Parser) parseList() ast.NodeRef {
	node := ast.ListNode{
		LiteralRefsStart: uint32(len(p.ctx.LiteralRefs)),
	}
	p.nextToken() // skip '['
	for !p.curTokenIs(lexer.RBRACKET) && !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.COMMA) {
			p.nextToken()
			continue
		}
		p.ctx.AddLiteralRef(p.parseLiteral())
	}
	if p.curTokenIs(lexer.RBRACKET) { p.nextToken() }
	node.LiteralRefsEnd = uint32(len(p.ctx.LiteralRefs))
	return p.ctx.AddListNode(node)
}

func (p *Parser) parseLiteral() ast.NodeRef {
	node := ast.LiteralNode{}
	switch p.curToken.Type {
	case lexer.STRING_LIT:
		node.Type = ast.LiteralString
		node.ValueRef = p.ctx.AddString(p.curToken.Literal)
		p.nextToken()
	case lexer.NUMBER_LIT:
		node.Type = ast.LiteralNumber
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
		node.Type = ast.LiteralDict
		node.Ref = p.parseDict()
	case lexer.LBRACKET:
		node.Type = ast.LiteralList
		node.Ref = p.parseList()
	default:
		p.curError("expected literal")
		p.nextToken()
	}
	return p.ctx.AddLiteralNode(node)
}
