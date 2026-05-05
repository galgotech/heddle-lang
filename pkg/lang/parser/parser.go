package parser

import (
	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

// ParserError captures syntax violations with precise source location metadata.
type ParserError struct {
	Message string
	Line    int
	Column  int
}

// Parser implements a recursive descent parser for the Heddle DSL.
// It leverages an ASTContext for pointerless node storage, ensuring GC-efficient
// representation and memory locality. It supports arbitrary lookahead via an
// internal token buffer.
type Parser struct {
	l          *lexer.Lexer
	curToken   lexer.Token   // Current token being evaluated
	peekTokens []lexer.Token // Lookahead buffer for predictive parsing
	ctx        *ast.ASTContext
	errors     []ParserError
}

// New initializes a parser with the provided lexer and AST context.
func New(l *lexer.Lexer, ctx *ast.ASTContext) *Parser {
	p := &Parser{
		l:   l,
		ctx: ctx,
	}
	// Initial state: curToken is empty, buffer is empty.
	// Prime the buffer and current token.
	p.nextToken()
	return p
}

// peek returns the token at the specified lookahead distance (0 for next, 1 for after, etc).
func (p *Parser) peek(n int) lexer.Token {
	for len(p.peekTokens) <= n {
		p.peekTokens = append(p.peekTokens, p.l.NextToken())
	}
	return p.peekTokens[n]
}

// nextToken advances the parser state by one token.
func (p *Parser) nextToken() {
	p.curToken = p.peek(0)
	if len(p.peekTokens) > 0 {
		p.peekTokens = p.peekTokens[1:]
	}
}

// curTokenIs checks if the current token matches the specified type.
func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

// peekTokenIs checks if the next token matches the specified type.
func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peek(0).Type == t
}

// expectPeek verifies the next token's type and advances if it matches.
func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

// Errors returns the collection of syntax errors encountered during parsing.
func (p *Parser) Errors() []ParserError {
	return p.errors
}

// peekError records a type mismatch error at the lookahead position.
func (p *Parser) peekError(t lexer.TokenType) {
	pk := p.peek(0)
	p.errors = append(p.errors, ParserError{
		Message: string(t) + " vs " + string(pk.Type),
		Line:    pk.Line,
		Column:  pk.Column,
	})
}

// getPos retrieves the source position of the current token.
func (p *Parser) getPos() ast.Position {
	return ast.Position{
		Line: uint32(p.curToken.Line),
		Col:  uint32(p.curToken.Column),
	}
}

// getRange computes an AST range from a starting position to the current token.
func (p *Parser) getRange(start ast.Position) ast.Range {
	return ast.Range{
		Start: start,
		End:   p.getPos(),
	}
}

// Parse orchestrates the top-level parsing loop.
func (p *Parser) Parse() ast.ProgramNode {
	program := ast.ProgramNode{
		ImportRefsStart:   uint32(len(p.ctx.ImportRefs)),
		SchemaRefsStart:   uint32(len(p.ctx.SchemaRefs)),
		ResourceRefsStart: uint32(len(p.ctx.ResourceRefs)),
		StepRefsStart:     uint32(len(p.ctx.StepRefs)),
		HandlerRefsStart:  uint32(len(p.ctx.HandlerRefs)),
		WorkflowRefsStart: uint32(len(p.ctx.WorkflowRefs)),
	}

	for !p.curTokenIs(lexer.EOF) {
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}

		switch p.curToken.Type {
		case lexer.IMPORT:
			p.ctx.AddImportRef(p.parseImport())
		case lexer.SCHEMA:
			p.ctx.AddSchemaRef(p.parseSchema())
		case lexer.RESOURCE:
			p.ctx.AddResourceRef(p.parseResource())
		case lexer.STEP:
			p.ctx.AddStepRef(p.parseStepBinding())
		case lexer.HANDLER:
			p.ctx.AddHandlerRef(p.parseHandler())
		case lexer.WORKFLOW:
			p.ctx.AddWorkflowRef(p.parseWorkflow())
		default:
			p.nextToken()
		}
	}

	program.ImportRefsEnd = uint32(len(p.ctx.ImportRefs))
	program.SchemaRefsEnd = uint32(len(p.ctx.SchemaRefs))
	program.ResourceRefsEnd = uint32(len(p.ctx.ResourceRefs))
	program.StepRefsEnd = uint32(len(p.ctx.StepRefs))
	program.HandlerRefsEnd = uint32(len(p.ctx.HandlerRefs))
	program.WorkflowRefsEnd = uint32(len(p.ctx.WorkflowRefs))

	return program
}

func (p *Parser) parseImport() ast.NodeRef {
	node := ast.ImportNode{}
	if p.expectPeek(lexer.STRING_LIT) {
		node.PathRef = p.ctx.AddString(p.curToken.Literal)
	}
	if p.expectPeek(lexer.IDENT) {
		node.AliasRef = p.ctx.AddString(p.curToken.Literal)
	}
	return p.ctx.AddImportNode(node)
}

func (p *Parser) parseSchema() ast.NodeRef {
	start := p.getPos()
	node := ast.SchemaNode{}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if p.peekTokenIs(lexer.LBRACE) {
		p.nextToken()
		node.BlockRef = p.parseSchemaBlock()
	} else if p.peekTokenIs(lexer.ASSIGN) {
		p.nextToken()
		p.nextToken()
		node.RefRef = p.parseSchemaRef()
	}
	ref := p.ctx.AddSchemaNode(node)
	p.ctx.SetSchemaRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseSchemaBlock() ast.NodeRef {
	node := ast.SchemaBlockNode{
		FieldRefsStart: uint32(len(p.ctx.FieldRefs)),
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if !p.expectPeek(lexer.INDENT) {
		return 0
	}
	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}
		if p.curTokenIs(lexer.IDENT) {
			field := ast.SchemaFieldNode{
				NameRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.COLON) {
				if p.peekTokenIs(lexer.LBRACE) {
					p.nextToken()
					field.BlockRef = p.parseSchemaBlock()
				} else if p.isTypeToken(p.peek(0).Type) {
					p.nextToken()
					field.TypeRef = p.ctx.AddString(p.curToken.Literal)
				}
			}
			p.ctx.AddFieldRef(p.ctx.AddSchemaFieldNode(field))
		}
	}
	if p.expectPeek(lexer.DEDENT) {
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if p.expectPeek(lexer.RBRACE) {
	}
	node.FieldRefsEnd = uint32(len(p.ctx.FieldRefs))
	return p.ctx.AddSchemaBlockNode(node)
}

func (p *Parser) isTypeToken(t lexer.TokenType) bool {
	switch t {
	case lexer.IDENT, lexer.STRING, lexer.INT, lexer.BOOL, lexer.FLOAT, lexer.TIMESTAMP:
		return true
	default:
		return false
	}
}

func (p *Parser) parseSchemaRef() ast.NodeRef {
	ref := ast.SchemaRefNode{}
	if !p.curTokenIs(lexer.IDENT) {
		return 0
	}
	ident1 := p.curToken.Literal
	if p.peekTokenIs(lexer.DOT) {
		p.nextToken()
		p.nextToken()
		ref.ModuleRef = p.ctx.AddString(ident1)
		ref.NameRef = p.ctx.AddString(p.curToken.Literal)
	} else {
		ref.NameRef = p.ctx.AddString(ident1)
	}
	return p.ctx.AddSchemaRefNode(ref)
}

func (p *Parser) parseResource() ast.NodeRef {
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
	return p.ctx.AddResourceNode(node)
}

func (p *Parser) parseStepBinding() ast.NodeRef {
	start := p.getPos()
	node := ast.StepBindingNode{}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.COLON) {
		return 0
	}
	p.nextToken()
	node.SignatureRef = p.parseStepSignature()
	if !p.expectPeek(lexer.ASSIGN) {
		return 0
	}
	p.nextToken()
	node.RefRef = p.parseFunctionRef()
	ref := p.ctx.AddStepBindingNode(node)
	p.ctx.SetStepRange(ref, p.getRange(start))
	return ref
}

func (p *Parser) parseStepSignature() ast.NodeRef {
	sig := ast.StepSignatureNode{}
	if p.curTokenIs(lexer.VOID) {
	} else {
		sig.InputRef = p.parseSchemaRef()
	}
	if !p.expectPeek(lexer.ARROW) {
		return 0
	}
	p.nextToken()
	if p.curTokenIs(lexer.VOID) {
	} else {
		sig.OutputRef = p.parseSchemaRef()
	}
	return p.ctx.AddStepSignatureNode(sig)
}

func (p *Parser) parseFunctionRef() ast.NodeRef {
	fr := ast.FunctionRefNode{}
	if !p.curTokenIs(lexer.IDENT) {
		return 0
	}
	fr.ModuleRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.DOT) {
		return 0
	}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	fr.NameRef = p.ctx.AddString(p.curToken.Literal)
	return p.ctx.AddFunctionRefNode(fr)
}

func (p *Parser) parseHandler() ast.NodeRef {
	node := ast.HandlerNode{
		StatementRefsStart: uint32(len(p.ctx.StatementRefs)),
	}
	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)
	if !p.expectPeek(lexer.LBRACE) {
		return 0
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if !p.expectPeek(lexer.INDENT) {
		return 0
	}
	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}
		p.ctx.AddStatementRef(p.parsePipelineStatement())
	}
	if p.expectPeek(lexer.DEDENT) {
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if p.expectPeek(lexer.RBRACE) {
	}
	node.StatementRefsEnd = uint32(len(p.ctx.StatementRefs))
	return p.ctx.AddHandlerNode(node)
}

func (p *Parser) parseWorkflow() ast.NodeRef {
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
		}
	}
	if !p.expectPeek(lexer.LBRACE) {
		return 0
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if !p.expectPeek(lexer.INDENT) {
		return 0
	}
	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}
		p.ctx.AddStatementRef(p.parsePipelineStatement())
	}
	for p.peekTokenIs(lexer.DEDENT) {
		p.nextToken()
	}
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}
	if p.expectPeek(lexer.RBRACE) {
	}
	node.StatementRefsEnd = uint32(len(p.ctx.StatementRefs))
	return p.ctx.AddWorkflowNode(node)
}

func (p *Parser) parsePipelineStatement() ast.NodeRef {
	ps := ast.PipelineStatementNode{}
	ps.ExprRef = p.parsePipeChain()

	// Multi-token lookahead to handle assignment after varied whitespace
	i := 0
	for p.peek(i).Type == lexer.NEWLINE || p.peek(i).Type == lexer.INDENT || p.peek(i).Type == lexer.DEDENT {
		i++
	}
	if p.peek(i).Type == lexer.RANGLE {
		for j := 0; j < i; j++ {
			p.nextToken()
		}
		p.nextToken() // consume '>'
		if p.expectPeek(lexer.IDENT) {
			ps.AssignmentRef = p.ctx.AddString(p.curToken.Literal)
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
		if p.peekTokenIs(lexer.PIPE) {
			p.nextToken()
			p.nextToken()
			p.ctx.AddCallRef(p.parseCall())
		} else if p.peekTokenIs(lexer.NEWLINE) && p.isPipeOnNextLine() {
			i := 0
			for p.peek(i).Type == lexer.NEWLINE || p.peek(i).Type == lexer.INDENT {
				i++
			}
			if p.peek(i).Type == lexer.PIPE {
				for j := 0; j < i; j++ {
					p.nextToken()
				}
				p.nextToken()
				p.nextToken()
				p.ctx.AddCallRef(p.parseCall())
				continue
			}
			break
		} else {
			break
		}
	}
	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

func (p *Parser) isPipeOnNextLine() bool {
	i := 0
	for p.peek(i).Type == lexer.NEWLINE || p.peek(i).Type == lexer.INDENT {
		i++
	}
	return p.peek(i).Type == lexer.PIPE
}

func (p *Parser) parseCall() ast.NodeRef {
	start := p.getPos()
	node := ast.CallNode{}
	if p.curTokenIs(lexer.IDENT) {
		node.NameRef = p.ctx.AddString(p.curToken.Literal)
	}
	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken()
		if p.expectPeek(lexer.IDENT) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
		}
	}
	ref := p.ctx.AddCallNode(node)
	p.ctx.SetCallRange(ref, p.getRange(start))
	return ref
}
