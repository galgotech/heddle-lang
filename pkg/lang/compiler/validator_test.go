package compiler

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lang/ast"
	"github.com/galgotech/heddle-lang/pkg/lang/lexer"
	"github.com/galgotech/heddle-lang/pkg/lang/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_TypeMismatch(t *testing.T) {
	input := `
schema S1 {
    f: int
}
schema S2 {
    f: int
}

step stepA: S1 -> S2 = m.a
step stepB: S1 -> void = m.b

workflow main {
    stepA | stepB
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()
	
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestValidator_ValidChain(t *testing.T) {
	input := `
schema S1 {
    f: int
}
schema S2 {
    f: int
}

step stepA: S1 -> S2 = m.a
step stepC: S2 -> S2 = m.c

workflow main {
    stepA | stepC
}
`
	ctx := ast.AcquireASTContext()
	defer ast.ReleaseASTContext(ctx)

	l := lexer.New(input)
	p := parser.New(l, ctx)
	prog := p.Parse()
	require.Empty(t, p.Errors())

	v := NewValidator(prog, ctx)
	err := v.Validate()
	
	assert.NoError(t, err)
}
