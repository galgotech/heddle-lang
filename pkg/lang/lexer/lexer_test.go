package lexer

import (
	"testing"
)

func TestNextToken(t *testing.T) {
	input := `import "std" io

resource db = postgres.connect {
  host: "localhost"
}

step process = my_plugin.run

workflow main {
  [
    {
      "a": 1
    }
  ]
  > data

  data
    | process
    | (from users select {name, age})
    | (
        from users 
        select {name, age}
        filter name == "test"
    )
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
		{NEWLINE, "\n\n"},
		{RESOURCE, "resource"},
		{IDENT, "db"},
		{ASSIGN, "="},
		{IDENT, "postgres"},
		{DOT, "."},
		{IDENT, "connect"},
		{LBRACE, "{"},
		{NEWLINE, "\n"},
		{INDENT, "  "},
		{IDENT, "host"},
		{COLON, ":"},
		{STRING_LIT, "localhost"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACE, "}"},
		{NEWLINE, "\n\n"},
		{STEP, "step"},
		{IDENT, "process"},
		{ASSIGN, "="},
		{IDENT, "my_plugin"},
		{DOT, "."},
		{IDENT, "run"},
		{NEWLINE, "\n\n"},
		{WORKFLOW, "workflow"},
		{IDENT, "main"},
		{LBRACE, "{"},
		{NEWLINE, "\n"},
		{INDENT, "  "},
		{LBRACKET, "["},
		{NEWLINE, "\n"},
		{INDENT, "    "},
		{LBRACE, "{"},
		{NEWLINE, "\n"},
		{INDENT, "      "},
		{STRING_LIT, "a"},
		{COLON, ":"},
		{NUMBER_LIT, "1"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACE, "}"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACKET, "]"},
		{NEWLINE, "\n"},
		{RANGLE, ">"},
		{IDENT, "data"},
		{NEWLINE, "\n\n"},
		{IDENT, "data"},
		{NEWLINE, "\n"},
		{INDENT, "    "},
		{PIPE, "|"},
		{IDENT, "process"},
		{NEWLINE, "\n"},
		{PIPE, "|"},
		{PRQL_BLOCK, "(from users select {name, age})"},
		{NEWLINE, "\n"},
		{PIPE, "|"},
		{PRQL_BLOCK, "(\n        from users \n        select {name, age}\n        filter name == \"test\"\n    )"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RANGLE, ">"},
		{IDENT, "processed"},
		{DEDENT, ""},
		{NEWLINE, "\n"},
		{RBRACE, "}"},
		{EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q (literal=%q, line=%d)",
				i, tt.expectedType, tok.Type, tok.Literal, tok.Line)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q (line=%d)",
				i, tt.expectedLiteral, tok.Literal, tok.Line)
		}
	}
}

func TestPRQLBlockStandalone(t *testing.T) {
	input := `(from users)`
	l := New(input)

	tok := l.NextToken()
	if tok.Type != PRQL_BLOCK {
		t.Fatalf("expected PRQL_BLOCK, got %q", tok.Type)
	}
	if tok.Literal != "(from users)" {
		t.Fatalf("expected '(from users)', got %q", tok.Literal)
	}
}
