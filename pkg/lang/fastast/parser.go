package fastast

import (
	"strings"
)

// DummyToken represents a basic token for our dummy parser.
type DummyToken struct {
	Type    string
	Literal string
}

// DummyParser simulates a recursive descent parser.
type DummyParser struct {
	tokens []DummyToken
	pos    int
	ctx    *ASTContext
}

// NewDummyParser creates a new parser. It requires an ASTContext.
func NewDummyParser(tokens []DummyToken, ctx *ASTContext) *DummyParser {
	return &DummyParser{
		tokens: tokens,
		pos:    0,
		ctx:    ctx,
	}
}

func (p *DummyParser) current() DummyToken {
	if p.pos >= len(p.tokens) {
		return DummyToken{Type: "EOF", Literal: ""}
	}
	return p.tokens[p.pos]
}

func (p *DummyParser) peek() DummyToken {
	if p.pos+1 >= len(p.tokens) {
		return DummyToken{Type: "EOF", Literal: ""}
	}
	return p.tokens[p.pos+1]
}

func (p *DummyParser) advance() {
	if p.pos < len(p.tokens) {
		p.pos++
	}
}

// Parse parses the tokens and constructs an AST inside the ASTContext.
// It returns a ProgramNode.
func (p *DummyParser) Parse() ProgramNode {
	program := ProgramNode{
		DAGRefsStart: uint32(len(p.ctx.DAGRefs)),
	}

	for p.current().Type != "EOF" {
		if p.current().Type == "DAG" {
			dagRef := p.parseDAG()
			p.ctx.AddDAGRef(dagRef)
		} else {
			p.advance() // Skip unknown tokens
		}
	}

	program.DAGRefsEnd = uint32(len(p.ctx.DAGRefs))
	return program
}

func (p *DummyParser) parseDAG() NodeRef {
	p.advance() // consume "DAG"

	nameRef := StringRef{}
	if p.current().Type == "IDENT" {
		nameRef = p.ctx.AddString(p.current().Literal)
		p.advance()
	}

	if p.current().Type == "LBRACE" {
		p.advance()
	}

	dag := DAGNode{
		NameRef:       nameRef,
		TaskRefsStart: uint32(len(p.ctx.TaskRefs)),
	}

	for p.current().Type != "EOF" && p.current().Type != "RBRACE" {
		if p.current().Type == "TASK" {
			taskRef := p.parseTask()
			p.ctx.AddTaskRef(taskRef)
		} else {
			p.advance()
		}
	}

	dag.TaskRefsEnd = uint32(len(p.ctx.TaskRefs))

	if p.current().Type == "RBRACE" {
		p.advance()
	}

	return p.ctx.AddDAGNode(dag)
}

func (p *DummyParser) parseTask() NodeRef {
	p.advance() // consume "TASK"

	nameRef := StringRef{}
	if p.current().Type == "IDENT" {
		nameRef = p.ctx.AddString(p.current().Literal)
		p.advance()
	}

	cmdRef := StringRef{}
	if p.current().Type == "STRING" {
		// strip quotes if needed, for dummy parser just add the string
		literal := p.current().Literal
		literal = strings.Trim(literal, "\"")
		cmdRef = p.ctx.AddString(literal)
		p.advance()
	}

	task := TaskNode{
		NameRef:    nameRef,
		CommandRef: cmdRef,
	}

	return p.ctx.AddTaskNode(task)
}

// Helper to construct tokens easily for testing
func TokenizeDummy(input string) []DummyToken {
	var tokens []DummyToken
	words := strings.Fields(input)
	for _, w := range words {
		switch {
		case w == "dag":
			tokens = append(tokens, DummyToken{Type: "DAG", Literal: w})
		case w == "{":
			tokens = append(tokens, DummyToken{Type: "LBRACE", Literal: w})
		case w == "}":
			tokens = append(tokens, DummyToken{Type: "RBRACE", Literal: w})
		case w == "task":
			tokens = append(tokens, DummyToken{Type: "TASK", Literal: w})
		case strings.HasPrefix(w, "\""):
			tokens = append(tokens, DummyToken{Type: "STRING", Literal: w})
		default:
			tokens = append(tokens, DummyToken{Type: "IDENT", Literal: w})
		}
	}
	return tokens
}
