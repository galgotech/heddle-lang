package fastast

import (
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
)

// Parser represents the pointerless AST parser.
type Parser struct {
	l         *lexer.Lexer
	curToken  lexer.Token
	peekToken lexer.Token
	ctx       *ASTContext
}

// NewParser creates a new parser. It requires an ASTContext.
func NewParser(l *lexer.Lexer, ctx *ASTContext) *Parser {
	p := &Parser{
		l:   l,
		ctx: ctx,
	}
	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	return false
}

// Parse parses the source code and constructs an AST inside the ASTContext.
func (p *Parser) Parse() ProgramNode {
	program := ProgramNode{
		ImportRefsStart:   uint32(len(p.ctx.ImportRefs)),
		SchemaRefsStart:   uint32(len(p.ctx.SchemaRefs)),
		ResourceRefsStart: uint32(len(p.ctx.ResourceRefs)),
		StepRefsStart:     uint32(len(p.ctx.StepRefs)),
		HandlerRefsStart:  uint32(len(p.ctx.HandlerRefs)),
		WorkflowRefsStart: uint32(len(p.ctx.WorkflowRefs)),
	}

	for !p.curTokenIs(lexer.EOF) {
		// Skip leading newlines
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

func (p *Parser) parseImport() NodeRef {
	node := ImportNode{}

	if p.expectPeek(lexer.STRING_LIT) {
		node.PathRef = p.ctx.AddString(p.curToken.Literal)
	}

	if p.expectPeek(lexer.IDENT) {
		node.AliasRef = p.ctx.AddString(p.curToken.Literal)
	}

	return p.ctx.AddImportNode(node)
}

func (p *Parser) parseSchema() NodeRef {
	node := SchemaNode{}

	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)

	if p.peekTokenIs(lexer.LBRACE) {
		p.nextToken()
		node.BlockRef = p.parseSchemaBlock()
	} else if p.peekTokenIs(lexer.ASSIGN) {
		p.nextToken() // consume =
		p.nextToken() // move to start of schema ref
		node.RefRef = p.parseSchemaRef()
	}

	return p.ctx.AddSchemaNode(node)
}

func (p *Parser) parseSchemaBlock() NodeRef {
	node := SchemaBlockNode{
		FieldRefsStart: uint32(len(p.ctx.FieldRefs)),
	}

	// Skip potential NEWLINE before INDENT
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
			field := SchemaFieldNode{
				NameRef: p.ctx.AddString(p.curToken.Literal),
			}
			if p.expectPeek(lexer.COLON) {
				if p.peekTokenIs(lexer.LBRACE) {
					p.nextToken()
					field.BlockRef = p.parseSchemaBlock()
				} else if p.expectPeek(lexer.IDENT) || p.expectPeek(lexer.STRING) || p.expectPeek(lexer.INT) || p.expectPeek(lexer.BOOL) || p.expectPeek(lexer.FLOAT) || p.expectPeek(lexer.TIMESTAMP) {
					field.TypeRef = p.ctx.AddString(p.curToken.Literal)
				}
			}
			p.ctx.AddFieldRef(p.ctx.AddSchemaFieldNode(field))
		}
	}

	if p.expectPeek(lexer.DEDENT) {
		// consume dedent
	}

	// Skip potential NEWLINE after DEDENT
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if p.expectPeek(lexer.RBRACE) {
		// consume rbrace
	}

	node.FieldRefsEnd = uint32(len(p.ctx.FieldRefs))
	return p.ctx.AddSchemaBlockNode(node)
}

func (p *Parser) parseSchemaRef() NodeRef {
	ref := SchemaRefNode{}

	if !p.curTokenIs(lexer.IDENT) {
		return 0
	}

	ident1 := p.curToken.Literal

	if p.peekTokenIs(lexer.DOT) {
		p.nextToken() // consume .
		p.nextToken() // consume next IDENT
		ref.ModuleRef = p.ctx.AddString(ident1)
		ref.NameRef = p.ctx.AddString(p.curToken.Literal)
	} else {
		ref.NameRef = p.ctx.AddString(ident1)
	}

	return p.ctx.AddSchemaRefNode(ref)
}

func (p *Parser) parseResource() NodeRef {
	node := ResourceNode{}

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

func (p *Parser) parseStepBinding() NodeRef {
	node := StepBindingNode{}

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

	return p.ctx.AddStepBindingNode(node)
}

func (p *Parser) parseStepSignature() NodeRef {
	sig := StepSignatureNode{}

	if p.curTokenIs(lexer.VOID) {
		// Special value for void
	} else {
		sig.InputRef = p.parseSchemaRef()
	}

	if !p.expectPeek(lexer.ARROW) {
		return 0
	}

	p.nextToken()
	if p.curTokenIs(lexer.VOID) {
		// Special value
	} else {
		sig.OutputRef = p.parseSchemaRef()
	}

	return p.ctx.AddStepSignatureNode(sig)
}

func (p *Parser) parseFunctionRef() NodeRef {
	fr := FunctionRefNode{}

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

	// Resource/config skipped for now to keep it simple

	return p.ctx.AddFunctionRefNode(fr)
}

func (p *Parser) parseHandler() NodeRef {
	node := HandlerNode{
		StatementRefsStart: uint32(len(p.ctx.StatementRefs)),
	}

	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)

	if !p.expectPeek(lexer.LBRACE) {
		return 0
	}

	// Skip NEWLINEs and INDENT
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

func (p *Parser) parseWorkflow() NodeRef {
	node := WorkflowNode{
		StatementRefsStart: uint32(len(p.ctx.StatementRefs)),
	}

	if !p.expectPeek(lexer.IDENT) {
		return 0
	}
	node.NameRef = p.ctx.AddString(p.curToken.Literal)

	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // consume ?
		if p.expectPeek(lexer.IDENT) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
		}
	}

	if !p.expectPeek(lexer.LBRACE) {
		return 0
	}

	// Skip NEWLINEs and INDENT
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

	// Consume all DEDENTs
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

func (p *Parser) parsePipelineStatement() NodeRef {
	ps := PipelineStatementNode{}

	// Simplified: only pipe chains for now
	ps.ExprRef = p.parsePipeChain()

	if p.peekTokenIs(lexer.RANGLE) {
		p.nextToken() // move to >
		if p.expectPeek(lexer.IDENT) {
			ps.AssignmentRef = p.ctx.AddString(p.curToken.Literal)
		}
	}

	return p.ctx.AddPipelineStatementNode(ps)
}

func (p *Parser) parsePipeChain() NodeRef {
	node := PipeChainNode{
		CallRefsStart: uint32(len(p.ctx.CallRefs)),
	}

	p.ctx.AddCallRef(p.parseCall())

	for {
		if p.peekTokenIs(lexer.PIPE) {
			p.nextToken() // |
			p.nextToken() // start of call
			p.ctx.AddCallRef(p.parseCall())
		} else if p.peekTokenIs(lexer.NEWLINE) && p.isPipeOnNextLine() {
			for p.peekTokenIs(lexer.NEWLINE) || p.peekTokenIs(lexer.INDENT) {
				p.nextToken()
			}
			if p.peekTokenIs(lexer.PIPE) {
				p.nextToken() // |
				p.nextToken() // start of call
				p.ctx.AddCallRef(p.parseCall())
			} else {
				break
			}
		} else {
			break
		}
	}

	node.CallRefsEnd = uint32(len(p.ctx.CallRefs))
	return p.ctx.AddPipeChainNode(node)
}

func (p *Parser) isPipeOnNextLine() bool {
	// Dummy implementation for now, in a real parser we'd need more lookahead
	return false
}

func (p *Parser) parseCall() NodeRef {
	node := CallNode{}

	if p.curTokenIs(lexer.IDENT) {
		node.NameRef = p.ctx.AddString(p.curToken.Literal)
	}

	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // consume ?
		if p.expectPeek(lexer.IDENT) {
			node.TrapRef = p.ctx.AddString(p.curToken.Literal)
		}
	}

	return p.ctx.AddCallNode(node)
}
