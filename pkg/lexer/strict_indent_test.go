package lexer

import (
	"testing"
)

func TestStrictIndentation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:     "mixed tabs and spaces in same line",
			input:    "workflow main {\n\t  step1\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, ILLEGAL},
		},
		{
			name:     "conflicting indentation style in file",
			input:    "workflow main {\n    step1\n\tstep2\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, INDENT, IDENT, NEWLINE, ILLEGAL},
		},
		{
			name:     "unindent mismatch",
			input:    "workflow main {\n    step1\n  step2\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, INDENT, IDENT, DEDENT, ILLEGAL, NEWLINE},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.input)
			for i, expectedType := range tt.expected {
				tok := l.NextToken()
				if tok.Type != expectedType {
					t.Errorf("test %s, token %d: expected %v, got %v (%s)", tt.name, i, expectedType, tok.Type, tok.Literal)
				}
			}
		})
	}
}
