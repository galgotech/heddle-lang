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
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	// 1. Lexing & Parsing
	l := lexer.New(source)
	p := parser.New(l, ctx)
	program := p.Parse()

	// 2. Semantic Validation
	v := NewValidator(program, ctx)
	if err := v.Validate(); err != nil {
		return nil, err
	}

	// 3. Lowering
	lowerer := NewLowerer(ctx)
	return lowerer.Lower(program)
}

// CompileFiles takes multiple Heddle source files and compiles them into a single IR.
func (c *Compiler) CompileFiles(paths []string) (*ir.ProgramIR, error) {
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	var wg sync.WaitGroup
	results := make([]ast.ProgramNode, len(paths))
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
			p_parser := parser.New(l, ctx)
			program := p_parser.Parse()

			results[idx] = program
		}(i, path)
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		return nil, <-errors
	}

	// Merge all programs into one
	combinedProgram := ast.ProgramNode{
		ImportRefsStart:   uint32(len(ctx.ImportRefs)),
		SchemaRefsStart:   uint32(len(ctx.SchemaRefs)),
		ResourceRefsStart: uint32(len(ctx.ResourceRefs)),
		StepRefsStart:     uint32(len(ctx.StepRefs)),
		HandlerRefsStart:  uint32(len(ctx.HandlerRefs)),
		WorkflowRefsStart: uint32(len(ctx.WorkflowRefs)),
	}

	for range results {
		// In a real implementation, we'd need to copy/adjust references
		// For now, let's just assume they were all parsed into the same ctx
		// and we can just extend the ranges.
	}
	// This merge logic is simplified and might need more work for a production compiler
	combinedProgram.ImportRefsEnd = uint32(len(ctx.ImportRefs))
	combinedProgram.SchemaRefsEnd = uint32(len(ctx.SchemaRefs))
	combinedProgram.ResourceRefsEnd = uint32(len(ctx.ResourceRefs))
	combinedProgram.StepRefsEnd = uint32(len(ctx.StepRefs))
	combinedProgram.HandlerRefsEnd = uint32(len(ctx.HandlerRefs))
	combinedProgram.WorkflowRefsEnd = uint32(len(ctx.WorkflowRefs))

	return c.CompileAST(combinedProgram, ctx)
}

// CompileAST takes an AST and lowers it into IR.
func (c *Compiler) CompileAST(program ast.ProgramNode, ctx *ast.ASTContext) (*ir.ProgramIR, error) {
	// 1. Semantic Validation
	v := NewValidator(program, ctx)
	if err := v.Validate(); err != nil {
		return nil, err
	}

	// 2. Lowering
	lowerer := NewLowerer(ctx)
	return lowerer.Lower(program)
}
