package lexer

import (
	"testing"
)

func TestSpecKeywords(t *testing.T) {
	input := "import resource step handler workflow true false null int float string boolean"
	l := New(input)

	expected := []struct {
		expectedType TokenType
		expectedLit  string
	}{
		{IMPORT, "import"},
		{RESOURCE, "resource"},
		{STEP, "step"},
		{HANDLER, "handler"},
		{WORKFLOW, "workflow"},
		{TRUE, "true"},
		{FALSE, "false"},
		{NULL, "null"},
		{INT, "int"},
		{FLOAT, "float"},
		{STRING, "string"},
		{BOOLEAN, "boolean"},
		{EOF, ""},
	}

	for i, tt := range expected {
		tok := l.NextToken()
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q", i, tt.expectedType, tok.Type)
		}
		if tok.Literal != tt.expectedLit {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q", i, tt.expectedLit, tok.Literal)
		}
	}
}

func TestRemovedKeywordsAndOperators(t *testing.T) {
	input := "schema void timestamp bool ->"
	l := New(input)

	// These should now be IDENT or ILLEGAL (for ->)
	expected := []struct {
		expectedType TokenType
	}{
		{IDENT},   // schema
		{IDENT},   // void
		{IDENT},   // timestamp
		{IDENT},   // bool
		{ILLEGAL}, // -> (since - followed by > is no longer ARROW)
	}

	for i, tt := range expected {
		tok := l.NextToken()
		if tok.Type != tt.expectedType {
			t.Errorf("tests[%d] - tokentype wrong. expected=%q, got=%q (lit: %q)", i, tt.expectedType, tok.Type, tok.Literal)
		}
	}
}

func TestHyphenatedIdentifier(t *testing.T) {
	input := "my-step-name"
	l := New(input)

	tok := l.NextToken()
	if tok.Type != IDENT {
		t.Fatalf("expected IDENT, got %q", tok.Type)
	}
	if tok.Literal != "my-step-name" {
		t.Fatalf("expected 'my-step-name', got %q", tok.Literal)
	}
}

func TestStandalonePRQLBlock(t *testing.T) {
	input := "(from users select id)"
	l := New(input)

	tok := l.NextToken()
	if tok.Type != PRQL_BLOCK {
		t.Fatalf("expected PRQL_BLOCK, got %q", tok.Type)
	}
}

func TestStrictLayout(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:  "same-line pipe (now allowed by lexer, enforced by parser)",
			input: "step1 | step2",
			expected: []TokenType{IDENT, PIPE, IDENT},
		},
		{
			name:  "same-line assignment (now allowed by lexer, enforced by parser)",
			input: "step1 > output",
			expected: []TokenType{IDENT, RANGLE, IDENT},
		},
		{
			name:     "valid new-line pipe",
			input:    "step1\n| step2",
			expected: []TokenType{IDENT, NEWLINE, PIPE, IDENT},
		},
		{
			name:     "valid new-line assignment",
			input:    "step1\n> output",
			expected: []TokenType{IDENT, NEWLINE, RANGLE, IDENT},
		},
		{
			name:     "invalid single-line block (now allowed by lexer, enforced by parser)",
			input:    "resource x = y { host: \"z\" }",
			expected: []TokenType{RESOURCE, IDENT, ASSIGN, IDENT, LBRACE, IDENT, COLON, STRING_LIT, RBRACE},
		},
		{
			name:     "comment skipping",
			input:    "// this is a comment\nworkflow main {\n  step1 // inline comment\n}",
			expected: []TokenType{NEWLINE, WORKFLOW, IDENT, LBRACE, NEWLINE, INDENT, IDENT, DEDENT, NEWLINE, RBRACE},
		},
		{
			name:     "non-indented block content (now allowed by lexer, enforced by parser)",
			input:    "resource x = y {\nhost: \"z\"\n}",
			expected: []TokenType{RESOURCE, IDENT, ASSIGN, IDENT, LBRACE, NEWLINE, IDENT, COLON, STRING_LIT, NEWLINE, RBRACE},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.input)
			for i, expectedType := range tt.expected {
				tok := l.NextToken()
				if tok.Type != expectedType {
					t.Errorf("token %d: expected %v, got %v (%s)", i, expectedType, tok.Type, tok.Literal)
				}
			}
		})
	}
}
