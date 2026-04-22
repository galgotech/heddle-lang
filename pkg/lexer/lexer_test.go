package lexer

import (
	"testing"
)

func TestNextToken(t *testing.T) {
	input := `import "std" io
schema User {
  id: int
  name: string
}

resource db = postgres.connect
step process: void -> void = my_plugin.run

workflow main {
  [{"a": 1}]
  > data

  data
    | process
  > processed
}
`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{IMPORT, "import"},
		{STRING_LIT, "std"},
		{IDENT, "io"},
		{NEWLINE, "\n"},
		{SCHEMA, "schema"},
		{IDENT, "User"},
		{LBRACE, "{"},
		{NEWLINE, "\n"},
		{INDENT, "  "},
		{IDENT, "id"},
		{COLON, ":"},
		{INT, "int"},
		{NEWLINE, "\n"},
		{IDENT, "name"},
		{COLON, ":"},
		{STRING, "string"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACE, "}"},
		{NEWLINE, "\n"},
		{NEWLINE, "\n"},
		{RESOURCE, "resource"},
		{IDENT, "db"},
		{ASSIGN, "="},
		{IDENT, "postgres"},
		{DOT, "."},
		{IDENT, "connect"},
		{NEWLINE, "\n"},
		{STEP, "step"},
		{IDENT, "process"},
		{COLON, ":"},
		{VOID, "void"},
		{ARROW, "->"},
		{VOID, "void"},
		{ASSIGN, "="},
		{IDENT, "my_plugin"},
		{DOT, "."},
		{IDENT, "run"},
		{NEWLINE, "\n"},
		{NEWLINE, "\n"},
		{WORKFLOW, "workflow"},
		{IDENT, "main"},
		{LBRACE, "{"},
		{NEWLINE, "\n"},
		{INDENT, "  "},
		{LBRACKET, "["},
		{LBRACE, "{"},
		{STRING_LIT, "a"},
		{COLON, ":"},
		{NUMBER_LIT, "1"},
		{RBRACE, "}"},
		{RBRACKET, "]"},
		{NEWLINE, "\n"},
		{RANGLE, ">"},
		{IDENT, "data"},
		{NEWLINE, "\n"},
		{NEWLINE, "\n"},
		{IDENT, "data"},
		{NEWLINE, "\n"},
		{INDENT, "    "},
		{PIPE, "|"},
		{IDENT, "process"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RANGLE, ">"},
		{IDENT, "processed"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACE, "}"},
		{NEWLINE, "\n"},
		{EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestPRQLAndComments(t *testing.T) {
	input := `// This is a comment
step q = (from users | select {name, age})
`
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{NEWLINE, "\n"},
		{STEP, "step"},
		{IDENT, "q"},
		{ASSIGN, "="},
		{PRQL_BLOCK, "(from users | select {name, age})"},
		{NEWLINE, "\n"},
		{EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}
