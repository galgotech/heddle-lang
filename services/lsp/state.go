package main

import (
	"sync"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/compiler"
	"github.com/galgotech/heddle-lang/pkg/lexer"
	"github.com/galgotech/heddle-lang/pkg/parser"
)

type Document struct {
	Text      string
	Program   *ast.Program
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

func (s *State) UpdateDocument(uri string, text string) (*ast.Program, []parser.ParserError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l := lexer.New(text)
	p := parser.New(l)
	program := p.Parse()

	v := compiler.NewValidator(program)
	// We don't care about validation errors here for the state,
	// they will be handled by publishDiagnostics.
	_ = v.Validate()

	s.documents[uri] = &Document{
		Text:      text,
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
