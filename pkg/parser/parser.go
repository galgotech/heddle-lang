package parser

import (
	"fmt"
	"strconv"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/lexer"
)

// Parser represents the Heddle language parser.
type Parser struct {
	l      *lexer.Lexer
	errors []string

	curToken  lexer.Token
	peekToken lexer.Token

	tokens []lexer.Token
	pos    int
}

// New creates a new instance of the Parser.
func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
		tokens: []lexer.Token{},
		pos:    0,
	}

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.getToken(p.pos)
	p.pos++
}

func (p *Parser) getToken(i int) lexer.Token {
	for i >= len(p.tokens) {
		p.tokens = append(p.tokens, p.l.NextToken())
	}
	return p.tokens[i]
}

func (p *Parser) peekTokenN(n int) lexer.Token {
	// n=1 is peekToken
	return p.getToken(p.pos + n - 1)
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
	p.peekError(t)
	return false
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) peekError(t lexer.TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead at line %d, col %d",
		t, p.peekToken.Type, p.peekToken.Line, p.peekToken.Column)
	p.errors = append(p.errors, msg)
}

// Parse takes source code and returns an AST.
func (p *Parser) Parse() *ast.Program {
	program := &ast.Program{}
	program.Statements = []ast.Statement{}

	for !p.curTokenIs(lexer.EOF) {
		// Skip leading newlines
		if p.curTokenIs(lexer.NEWLINE) {
			p.nextToken()
			continue
		}

		stmt := p.parseStatement()
		if stmt != nil {
			program.Statements = append(program.Statements, stmt)
		}
		p.nextToken()
	}

	return program
}

func (p *Parser) parseStatement() ast.Statement {
	switch p.curToken.Type {
	case lexer.IMPORT:
		return p.parseImportStatement()
	case lexer.SCHEMA:
		return p.parseSchemaDefinition()
	case lexer.RESOURCE:
		return p.parseResourceBinding()
	case lexer.STEP:
		return p.parseStepBinding()
	case lexer.HANDLER:
		return p.parseHandlerDefinition()
	case lexer.WORKFLOW:
		return p.parseWorkflowDefinition()
	default:
		// Could be a pipeline statement (starts with [ or IDENT or signature)
		return p.parsePipelineStatement()
	}
}

func (p *Parser) parseImportStatement() *ast.ImportStatement {
	stmt := &ast.ImportStatement{Token: p.curToken}

	if !p.expectPeek(lexer.STRING_LIT) {
		return nil
	}

	stmt.Path = &ast.StringLiteral{Token: p.curToken, Value: p.curToken.Literal}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}

	stmt.Alias = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	return stmt
}

func (p *Parser) parseSchemaDefinition() *ast.SchemaDefinition {
	stmt := &ast.SchemaDefinition{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}

	stmt.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if p.peekTokenIs(lexer.LBRACE) {
		p.nextToken()
		stmt.Block = p.parseSchemaBlock()
	} else if p.peekTokenIs(lexer.ASSIGN) {
		p.nextToken() // consume =
		p.nextToken() // move to start of schema ref
		stmt.Ref = p.parseSchemaRef()
	} else {
		p.peekError(lexer.LBRACE) // or ASSIGN
		return nil
	}

	return stmt
}

func (p *Parser) parseSchemaBlock() *ast.SchemaBlock {
	sb := &ast.SchemaBlock{Token: p.curToken, Fields: make(map[string]interface{})}

	// grammar: "{" _NL _INDENT (schema_block_pair (","? _NL schema_block_pair)* ","? _NL?)? _DEDENT _NL "}"

	// Skip potential NEWLINE before INDENT
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.INDENT) {
		return nil
	}

	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}

		if p.curTokenIs(lexer.IDENT) {
			name := p.curToken.Literal
			if !p.expectPeek(lexer.COLON) {
				return nil
			}

			if p.peekTokenIs(lexer.LBRACE) {
				p.nextToken()
				sb.Fields[name] = p.parseSchemaBlock()
			} else {
				p.nextToken()
				sb.Fields[name] = p.curToken.Literal // primitive type
			}
		}
	}

	if !p.expectPeek(lexer.DEDENT) {
		return nil
	}

	// Skip potential NEWLINE after DEDENT
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.RBRACE) {
		return nil
	}

	return sb
}

func (p *Parser) parseSchemaRef() *ast.SchemaRef {
	ref := &ast.SchemaRef{}

	if !p.curTokenIs(lexer.IDENT) {
		return nil
	}

	ident1 := &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if p.peekTokenIs(lexer.DOT) {
		p.nextToken() // consume .
		p.nextToken() // consume next IDENT
		ref.Module = ident1
		ref.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}
	} else {
		ref.Name = ident1
	}

	return ref
}

func (p *Parser) parseResourceBinding() *ast.ResourceBinding {
	stmt := &ast.ResourceBinding{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}
	stmt.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if !p.expectPeek(lexer.ASSIGN) {
		return nil
	}

	p.nextToken()
	stmt.Ref = p.parseFunctionRef()

	return stmt
}

func (p *Parser) parseStepBinding() *ast.StepBinding {
	stmt := &ast.StepBinding{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}
	stmt.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if !p.expectPeek(lexer.COLON) {
		return nil
	}

	p.nextToken()
	stmt.Signature = p.parseStepSignature()

	if !p.expectPeek(lexer.ASSIGN) {
		return nil
	}

	p.nextToken()
	stmt.Ref = p.parseFunctionRef()

	return stmt
}

func (p *Parser) parseHandlerDefinition() *ast.HandlerDefinition {
	hd := &ast.HandlerDefinition{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}
	hd.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if !p.expectPeek(lexer.LBRACE) {
		return nil
	}

	// Skip NEWLINEs and INDENT
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.INDENT) {
		return nil
	}

	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}

		var stmt ast.Statement
		if p.curTokenIs(lexer.ASTERISK) {
			capture := &ast.CaptureStatement{Token: p.curToken}
			p.nextToken()
			pipeline := p.parsePipelineStatement()
			if pipeline != nil {
				// We need a way to group capture + pipeline in HandlerDefinition
				// For now let's just add them separately or update AST
				hd.Statements = append(hd.Statements, capture)
				hd.Statements = append(hd.Statements, pipeline)
			}
		} else {
			stmt = p.parsePipelineStatement()
			if stmt != nil {
				hd.Statements = append(hd.Statements, stmt)
			}
		}
	}

	if !p.expectPeek(lexer.DEDENT) {
		return nil
	}

	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.RBRACE) {
		return nil
	}

	return hd
}

func (p *Parser) parseWorkflowDefinition() *ast.WorkflowDefinition {
	wd := &ast.WorkflowDefinition{Token: p.curToken}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}
	wd.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // consume ?
		if !p.expectPeek(lexer.IDENT) {
			return nil
		}
		wd.TrapHandler = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}
	}

	if !p.expectPeek(lexer.LBRACE) {
		return nil
	}

	// Skip NEWLINEs and INDENT
	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.INDENT) {
		return nil
	}

	for !p.peekTokenIs(lexer.DEDENT) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}

		stmt := p.parsePipelineStatement()
		if stmt != nil {
			wd.Statements = append(wd.Statements, stmt)
		}
	}

	// Consume all DEDENTs until we reach the block closing level
	for p.peekTokenIs(lexer.DEDENT) {
		p.nextToken()
	}

	for p.peekTokenIs(lexer.NEWLINE) {
		p.nextToken()
	}

	if !p.expectPeek(lexer.RBRACE) {
		return nil
	}

	return wd
}

func (p *Parser) parsePipelineStatement() *ast.PipelineStatement {
	ps := &ast.PipelineStatement{}

	if p.curTokenIs(lexer.LBRACKET) {
		ps.Expression = p.parseDataframe()
	} else {
		ps.Expression = p.parsePipeChain()
	}

	// Assignment can be on the same line or next line
	if p.peekTokenIs(lexer.RANGLE) {
		p.nextToken() // move to >
		if !p.expectPeek(lexer.IDENT) {
			return nil
		}
		ps.Assignment = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}
	} else if (p.peekTokenIs(lexer.NEWLINE) || p.peekTokenIs(lexer.DEDENT)) && p.isAssignmentOnNextLine() {
		for p.peekTokenIs(lexer.NEWLINE) || p.peekTokenIs(lexer.INDENT) || p.peekTokenIs(lexer.DEDENT) {
			p.nextToken()
		}
		if p.peekTokenIs(lexer.RANGLE) {
			p.nextToken() // move to >
			p.nextToken() // move to identifier
			ps.Assignment = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}
		}
	}
	return ps
}

func (p *Parser) isAssignmentOnNextLine() bool {
	for i := 1; ; i++ {
		tok := p.peekTokenN(i)
		if tok.Type == lexer.NEWLINE || tok.Type == lexer.INDENT || tok.Type == lexer.DEDENT {
			continue
		}
		return tok.Type == lexer.RANGLE
	}
}

func (p *Parser) parseDataframe() *ast.Dataframe {
	df := &ast.Dataframe{Token: p.curToken}
	// Simplified parsing for now
	for !p.peekTokenIs(lexer.RBRACKET) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
	}
	p.expectPeek(lexer.RBRACKET)
	return df
}

func (p *Parser) parsePipeChain() *ast.PipeChain {
	pc := &ast.PipeChain{}
	pc.Calls = append(pc.Calls, p.parseCallExpression())

	for {
		if p.peekTokenIs(lexer.PIPE) {
			p.nextToken() // curToken = |
			p.nextToken() // curToken = start of call
			pc.Calls = append(pc.Calls, p.parseCallExpression())
		} else if p.peekTokenIs(lexer.NEWLINE) && p.isPipeOnNextLine() {
			for p.peekTokenIs(lexer.NEWLINE) || p.peekTokenIs(lexer.INDENT) {
				p.nextToken()
			}
			if p.peekTokenIs(lexer.PIPE) {
				p.nextToken() // move to |
				p.nextToken() // move to start of call
				pc.Calls = append(pc.Calls, p.parseCallExpression())
			} else {
				break
			}
		} else {
			break
		}
	}

	return pc
}

func (p *Parser) isPipeOnNextLine() bool {
	for i := 1; ; i++ {
		tok := p.peekTokenN(i)
		if tok.Type == lexer.NEWLINE || tok.Type == lexer.INDENT {
			continue
		}
		return tok.Type == lexer.PIPE
	}
}

func (p *Parser) parseCallExpression() *ast.CallExpression {
	ce := &ast.CallExpression{}

	if p.isAnonymousStep() {
		ce.Step = p.parseAnonymousStepExpression()
	} else if p.curTokenIs(lexer.IDENT) {
		ce.Step = &ast.StepCall{
			Token: p.curToken,
			Name:  &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal},
		}
	} else if p.curTokenIs(lexer.PRQL_BLOCK) {
		ce.Step = &ast.PRQLExpression{
			Token: p.curToken,
			Value: p.curToken.Literal,
		}
	} else {
		msg := fmt.Sprintf("expected identifier or anonymous step, got %s at line %d, col %d",
			p.curToken.Type, p.curToken.Line, p.curToken.Column)
		p.errors = append(p.errors, msg)
		return nil
	}

	if p.peekTokenIs(lexer.QUESTION) {
		p.nextToken() // consume ?
		if !p.expectPeek(lexer.IDENT) {
			return nil
		}
		th := &ast.TrapHandler{Token: p.curToken}
		th.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}
		ce.TrapHandler = th
	}

	return ce
}

func (p *Parser) isAnonymousStep() bool {
	if p.curTokenIs(lexer.VOID) {
		return true
	}
	if p.curTokenIs(lexer.IDENT) {
		// Look ahead for -> or .
		// SchemaRef can be IDENT or IDENT.IDENT
		if p.peekTokenIs(lexer.ARROW) {
			return true
		}
		if p.peekTokenIs(lexer.DOT) {
			// Check if it's IDENT.IDENT ->
			if p.peekTokenN(2).Type == lexer.IDENT && p.peekTokenN(3).Type == lexer.ARROW {
				return true
			}
		}
	}
	return false
}

func (p *Parser) parseAnonymousStepExpression() *ast.AnonymousStepExpression {
	ase := &ast.AnonymousStepExpression{}
	ase.Signature = p.parseStepSignature()

	if !p.expectPeek(lexer.ASSIGN) {
		return nil
	}

	p.nextToken()
	if p.curTokenIs(lexer.PRQL_BLOCK) {
		ase.Ref = &ast.PRQLExpression{Token: p.curToken, Value: p.curToken.Literal}
	} else {
		ase.Ref = p.parseFunctionRef()
	}

	return ase
}

func (p *Parser) parseStepSignature() *ast.StepSignature {
	sig := &ast.StepSignature{}

	if p.curTokenIs(lexer.VOID) {
		sig.Input = &ast.VoidType{Token: p.curToken}
	} else {
		sig.Input = p.parseSchemaRef()
	}

	if !p.expectPeek(lexer.ARROW) {
		return nil
	}

	p.nextToken()
	if p.curTokenIs(lexer.VOID) {
		sig.Output = &ast.VoidType{Token: p.curToken}
	} else {
		sig.Output = p.parseSchemaRef()
	}

	return sig
}

func (p *Parser) parseFunctionRef() *ast.FunctionRef {
	fr := &ast.FunctionRef{}

	if !p.curTokenIs(lexer.IDENT) {
		return nil
	}
	fr.Module = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	if !p.expectPeek(lexer.DOT) {
		return nil
	}

	if !p.expectPeek(lexer.IDENT) {
		return nil
	}
	fr.Name = &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}

	// Optional: resource_ref <...> and function_config {...}
	if p.peekTokenIs(lexer.LANGLE) {
		// parse resource ref
		p.nextToken()
		fr.Resource = make(map[string]string)
		for !p.peekTokenIs(lexer.RANGLE) && !p.peekTokenIs(lexer.EOF) {
			p.nextToken()
			if p.curTokenIs(lexer.IDENT) {
				key := p.curToken.Literal
				if !p.expectPeek(lexer.ASSIGN) {
					return nil
				}
				if !p.expectPeek(lexer.IDENT) {
					return nil
				}
				fr.Resource[key] = p.curToken.Literal
				if p.peekTokenIs(lexer.COMMA) {
					p.nextToken()
				}
			}
		}
		p.expectPeek(lexer.RANGLE)
	}

	if p.peekTokenIs(lexer.LBRACE) {
		p.nextToken()
		fr.Config = p.parseDictionary()
	}

	return fr
}

func (p *Parser) parseDictionary() *ast.Dictionary {
	dict := &ast.Dictionary{Token: p.curToken, Pairs: make(map[string]ast.Expression)}

	for !p.peekTokenIs(lexer.RBRACE) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) || p.curTokenIs(lexer.INDENT) || p.curTokenIs(lexer.DEDENT) {
			continue
		}

		if p.curTokenIs(lexer.IDENT) {
			key := p.curToken.Literal
			if !p.expectPeek(lexer.COLON) {
				return nil
			}
			p.nextToken()
			dict.Pairs[key] = p.parseExpression()
			if p.peekTokenIs(lexer.COMMA) {
				p.nextToken()
			}
		}
	}
	p.expectPeek(lexer.RBRACE)
	return dict
}

func (p *Parser) parseExpression() ast.Expression {
	switch p.curToken.Type {
	case lexer.STRING_LIT:
		return &ast.StringLiteral{Token: p.curToken, Value: p.curToken.Literal}
	case lexer.NUMBER_LIT:
		val, _ := strconv.ParseFloat(p.curToken.Literal, 64)
		return &ast.NumberLiteral{Token: p.curToken, Value: val}
	case lexer.TRUE, lexer.FALSE:
		return &ast.BooleanLiteral{Token: p.curToken, Value: p.curToken.Type == lexer.TRUE}
	case lexer.NULL:
		return &ast.NullLiteral{Token: p.curToken}
	case lexer.LBRACE:
		return p.parseDictionary()
	case lexer.LBRACKET:
		// Could be list or dataframe, but in literal context it's a list
		return p.parseList()
	default:
		return nil
	}
}

func (p *Parser) parseList() *ast.List {
	l := &ast.List{Token: p.curToken}
	for !p.peekTokenIs(lexer.RBRACKET) && !p.peekTokenIs(lexer.EOF) {
		p.nextToken()
		if p.curTokenIs(lexer.NEWLINE) {
			continue
		}
		l.Elements = append(l.Elements, p.parseExpression())
		if p.peekTokenIs(lexer.COMMA) {
			p.nextToken()
		}
	}
	p.expectPeek(lexer.RBRACKET)
	return l
}
