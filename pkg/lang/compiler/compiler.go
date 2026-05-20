package compiler

import (
	"fmt"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
)

// Compiler represents the Heddle compiler.
type Compiler struct {
}

// Compile takes Heddle source code and compiles it into IR.
func (c *Compiler) Compile(source string) (ir.Program, error) {
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	// 1. Lexing & Parsing
	l := lexer.New(source)
	p := parser.New(l, ctx)
	program := p.Parse()
	if len(p.Errors()) > 0 {
		return ir.Program{}, fmt.Errorf("parser errors: %v", p.Errors())
	}

	// 2. Semantic Validation
	v := NewValidator(program, ctx, nil)
	if err := v.Validate(); err != nil {
		return ir.Program{}, err
	}

	// 3. Lowering
	lowerer := NewLowerer(ctx)
	return lowerer.Lower(program)
}

// New creates a new instance of the Compiler.
func New() *Compiler {
	return &Compiler{}
}
