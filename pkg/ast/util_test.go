package ast

import (
	"testing"

	"github.com/galgotech/heddle-lang/pkg/lexer"

	"github.com/stretchr/testify/assert"
)

func TestFindNodeAt(t *testing.T) {
	// Simple program: "resource s3 {}"
	// Line 1, Column 1-8 is 'resource', 10-11 is 's3', 13-14 is '{}'

	id := &Identifier{
		Token: lexer.Token{Type: lexer.IDENT, Literal: "s3", Line: 1, Column: 10},
		Value: "s3",
	}

	res := &ResourceBinding{
		Token: lexer.Token{Type: lexer.RESOURCE, Literal: "resource", Line: 1, Column: 1},
		Name:  id,
	}

	program := &Program{
		Statements: []Statement{res},
	}

	// 1. Exact match on Identifier
	found := FindNodeAt(program, 1, 10)
	assert.NotNil(t, found)
	assert.Equal(t, "s3", found.TokenLiteral())

	// 2. Exact match on Resource keyword
	found = FindNodeAt(program, 1, 1)
	assert.NotNil(t, found)
	assert.Equal(t, "resource", found.TokenLiteral())

	// 3. No match (out of range)
	found = FindNodeAt(program, 1, 20)
	assert.Nil(t, found)
}
