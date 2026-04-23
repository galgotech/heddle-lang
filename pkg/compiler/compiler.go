package compiler

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/ast"
	"github.com/galgotech/heddle-lang/pkg/ir"
	"github.com/galgotech/heddle-lang/pkg/lexer"
	"github.com/galgotech/heddle-lang/pkg/parser"
)

// Compiler represents the Heddle compiler.
type Compiler struct {
}

// New creates a new instance of the Compiler.
func New() *Compiler {
	return &Compiler{}
}

// Compile takes Heddle source code and compiles it into IR.
func (c *Compiler) Compile(source string) (*ir.ProgramIR, error) {
	// 1. Lexing & Parsing
	l := lexer.New(source)
	p := parser.New(l)
	program := p.Parse()

	if len(p.Errors()) > 0 {
		return nil, fmt.Errorf("parser errors: %v", p.Errors())
	}

	// 2. Semantic Validation
	v := NewValidator(program)
	if err := v.Validate(); err != nil {
		return nil, err
	}

	// 3. Lowering
	lowerer := NewLowerer()
	return lowerer.Lower(program)
}

// CompileAST takes an AST and lowers it into IR.
func (c *Compiler) CompileAST(program *ast.Program) (*ir.ProgramIR, error) {
	// 1. Semantic Validation
	v := NewValidator(program)
	if err := v.Validate(); err != nil {
		return nil, err
	}

	// 2. Lowering
	lowerer := NewLowerer()
	return lowerer.Lower(program)
}
