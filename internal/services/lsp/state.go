package lsp

import (
	"sync"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

type Document struct {
	Text      string
	Ctx       *ast.ASTContext
	Program   ast.ProgramNode
	Validator *compiler.Validator
}

type State struct {
	mu        sync.RWMutex
	documents map[string]*Document
}

func NewState() *State {
	return &State{
		documents: make(map[string]*Document),
	}
}

func (s *State) UpdateDocument(uri string, text string) (ast.ProgramNode, []parser.ParserError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release previous context if exists
	if old, ok := s.documents[uri]; ok {
		ast.ReleaseASTContext(old.Ctx)
	}

	ctx := ast.AcquireASTContext()
	l := lexer.New(text)
	p := parser.New(l, ctx)
	program := p.Parse()

	v := compiler.NewValidator(program, ctx)
	// We don't care about validation errors here for the state,
	// they will be handled by publishDiagnostics.
	_ = v.Validate()

	s.documents[uri] = &Document{
		Text:      text,
		Ctx:       ctx,
		Program:   program,
		Validator: v,
	}

	return program, p.Errors()
}

func (s *State) GetDocument(uri string) (*Document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	doc, ok := s.documents[uri]
	return doc, ok
}
