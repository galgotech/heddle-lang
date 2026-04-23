package compiler

import (
	"fmt"
	"os"
	"sync"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
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

// CompileFiles takes multiple Heddle source files and compiles them into a single IR.
func (c *Compiler) CompileFiles(paths []string) (*ir.ProgramIR, error) {
	var wg sync.WaitGroup
	results := make([]*ast.Program, len(paths))
	errors := make(chan error, len(paths))

	for i, path := range paths {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()

			content, err := os.ReadFile(p)
			if err != nil {
				errors <- fmt.Errorf("failed to read file %s: %w", p, err)
				return
			}

			l := lexer.New(string(content))
			p_parser := parser.New(l)
			program := p_parser.Parse()

			if len(p_parser.Errors()) > 0 {
				errors <- fmt.Errorf("parser errors in %s: %v", p, p_parser.Errors())
				return
			}

			results[idx] = program
		}(i, path)
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		return nil, <-errors
	}

	// Merge all programs into one
	combinedProgram := &ast.Program{
		Statements: []ast.Statement{},
	}

	for _, p := range results {
		if p != nil {
			combinedProgram.Statements = append(combinedProgram.Statements, p.Statements...)
		}
	}

	return c.CompileAST(combinedProgram)
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
