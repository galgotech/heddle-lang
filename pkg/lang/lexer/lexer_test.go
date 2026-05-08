package lexer

import (
	"testing"
)

func TestNextToken(t *testing.T) {
	input := `import "std/io" io

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
		expectedLine    int
		expectedColumn  int
		expectedStart   int
	}{
		{IMPORT, "import", 1, 1, 0},
		{STRING_LIT, "std/io", 1, 8, 7},
		{IDENT, "io", 1, 17, 16},
		{NEWLINE, "\n\n", 1, 19, 18},
		{RESOURCE, "resource", 3, 1, 20},
		{IDENT, "db", 3, 10, 29},
		{ASSIGN, "=", 3, 13, 32},
		{IDENT, "postgres", 3, 15, 34},
		{DOT, ".", 3, 23, 42},
		{IDENT, "connect", 3, 24, 43},
		{LBRACE, "{", 3, 32, 51},
		{NEWLINE, "\n", 3, 33, 52},
		{INDENT, "  ", 4, 1, 53},
		{IDENT, "host", 4, 3, 55},
		{COLON, ":", 4, 7, 59},
		{STRING_LIT, "localhost", 4, 9, 61},
		{DEDENT, "", 5, 1, 73},
		{NEWLINE, "\n", 4, 20, 72},
		{RBRACE, "}", 5, 1, 73},
		{NEWLINE, "\n\n", 5, 2, 74},
		{STEP, "step", 7, 1, 76},
		{IDENT, "process", 7, 6, 81},
		{ASSIGN, "=", 7, 14, 89},
		{IDENT, "my_plugin", 7, 16, 91},
		{DOT, ".", 7, 25, 100},
		{IDENT, "run", 7, 26, 101},
		{NEWLINE, "\n\n", 7, 29, 104},
		{WORKFLOW, "workflow", 9, 1, 106},
		{IDENT, "main", 9, 10, 115},
		{LBRACE, "{", 9, 15, 120},
		{NEWLINE, "\n", 9, 16, 121},
		{INDENT, "  ", 10, 1, 122},
		{LBRACKET, "[", 10, 3, 124},
		{NEWLINE, "\n", 10, 4, 125},
		{INDENT, "    ", 11, 1, 126},
		{LBRACE, "{", 11, 5, 130},
		{NEWLINE, "\n", 11, 6, 131},
		{INDENT, "      ", 12, 1, 132},
		{STRING_LIT, "a", 12, 7, 138},
		{COLON, ":", 12, 10, 141},
		{INT, "1", 12, 12, 143},
		{DEDENT, "", 13, 1, 149},
		{NEWLINE, "\n", 12, 13, 144},
		{RBRACE, "}", 13, 5, 149},
		{DEDENT, "", 14, 1, 153},
		{NEWLINE, "\n", 13, 6, 150},
		{RBRACKET, "]", 14, 3, 153},
		{NEWLINE, "\n", 14, 4, 154},
		{RANGLE, ">", 15, 3, 157},
		{IDENT, "data", 15, 5, 159},
		{NEWLINE, "\n\n", 15, 9, 163},
		{IDENT, "data", 17, 3, 167},
		{NEWLINE, "\n", 17, 7, 171},
		{INDENT, "    ", 18, 1, 172},
		{PIPE, "|", 18, 5, 176},
		{IDENT, "process", 18, 7, 178},
		{NEWLINE, "\n", 18, 14, 185},
		{PIPE, "|", 19, 5, 190},
		{PRQL_BLOCK, "(from users select {name, age})", 19, 7, 192},
		{NEWLINE, "\n", 19, 38, 223},
		{PIPE, "|", 20, 5, 228},
		{PRQL_BLOCK, "(\n        from users \n        select {name, age}\n        filter name == \"test\"\n    )", 20, 7, 230},
		{DEDENT, "", 25, 1, 317},
		{NEWLINE, "\n", 24, 7, 314},
		{RANGLE, ">", 25, 3, 317},
		{IDENT, "processed", 25, 5, 319},
		{DEDENT, "", 26, 1, 329},
		{NEWLINE, "\n", 25, 14, 328},
		{RBRACE, "}", 26, 1, 329},
		{EOF, "", 27, 1, 331},
	}

	l := New(input)
	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q (literal=%q, line=%d, col=%d)",
				i, tt.expectedType, tok.Type, tok.Literal, tok.Line, tok.Column)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q (line=%d, col=%d)",
				i, tt.expectedLiteral, tok.Literal, tok.Line, tok.Column)
		}

		if tok.Line != tt.expectedLine {
			t.Fatalf("tests[%d] - line wrong. expected=%d, got=%d (literal=%q, col=%d)",
				i, tt.expectedLine, tok.Line, tok.Literal, tok.Column)
		}

		if tok.Column != tt.expectedColumn {
			t.Fatalf("tests[%d] - column wrong. expected=%d, got=%d (literal=%q, line=%d)",
				i, tt.expectedColumn, tok.Column, tok.Literal, tok.Line)
		}

		if tok.Start != tt.expectedStart {
			t.Fatalf("tests[%d] - start position wrong. expected=%d, got=%d (literal=%q, line=%d, col=%d)",
				i, tt.expectedStart, tok.Start, tok.Literal, tok.Line, tok.Column)
		}
	}
}

func TestBasicTokens(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
	}{
		// Numbers
		{"123", INT},
		{"-123", INT},
		{"+123", INT},
		{"1.23", FLOAT},
		{"-1.23", FLOAT},
		{"+1.23", FLOAT},
		{".123", FLOAT},
		{"123.", FLOAT},

		// Operators
		{"=", ASSIGN},
		{"*", ASTERISK},
		{">", RANGLE},
		{"<", LANGLE},
		{"|", PIPE},
		{"?", QUESTION},
		{".", DOT},

		// Keywords
		{"import", IMPORT},
		{"resource", RESOURCE},
		{"step", STEP},
		{"handler", HANDLER},
		{"workflow", WORKFLOW},
		{"true", TRUE},
		{"false", FALSE},
		{"null", NULL},
		{"int", INT},
		{"float", FLOAT},
		{"string", STRING},
		{"boolean", BOOLEAN},

		// Punctuation
		{":", COLON},
		{"{", LBRACE},
		{"}", RBRACE},
		{")", RPAREN},
		{"[", LBRACKET},
		{"]", RBRACKET},
		{",", COMMA},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.NextToken()
		if tok.Type != tt.expectedType {
			t.Errorf("input %s: expected type %v, got %v", tt.input, tt.expectedType, tok.Type)
		}
	}
}

func TestPRQLBlock(t *testing.T) {
	input := `(from employees
filter start_date > @2021-01-01 
derive {
  gross_salary = salary + payroll_tax,
  gross_cost = gross_salary + benefits_cost
}
filter gross_cost > 0
group {title, country} (
  aggregate {
    average salary,
    sum     salary,
    average gross_salary,
    sum     gross_salary,
    average gross_cost,
    sum_gross_cost = sum gross_cost,
    ct = count this,
  }
)
sort {sum_gross_cost, -country}
filter ct > 2_000
take 20)`
	l := New(input)

	tok := l.NextToken()
	if tok.Type != PRQL_BLOCK {
		t.Fatalf("expected PRQL_BLOCK, got %q", tok.Type)
	}
	if tok.Literal != input {
		t.Fatalf("expected '(from users)', got %q", tok.Literal)
	}
}

func TestTokenSequences(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		// Indentation scenarios
		{
			name:     "mixed tabs and spaces in same line",
			input:    "workflow main {\n\t  step1\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, ILLEGAL},
		},
		{
			name:     "conflicting indentation style in file",
			input:    "workflow main {\n  step1\n\tstep2\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, INDENT, IDENT, NEWLINE, ILLEGAL},
		},
		{
			name:     "unindent mismatch",
			input:    "workflow main {\n  step1\n step2\n}",
			expected: []TokenType{WORKFLOW, IDENT, LBRACE, NEWLINE, INDENT, IDENT, DEDENT, ILLEGAL, NEWLINE},
		},

		// Invalid syntax scenarios
		{
			name:  "illegal character in workflow",
			input: "workflow main {\n  source @| process\n}",
			expected: []TokenType{
				WORKFLOW, IDENT, LBRACE, NEWLINE,
				INDENT, IDENT, ILLEGAL, PIPE, IDENT, DEDENT, NEWLINE,
				RBRACE, EOF,
			},
		},
		{
			name:  "unclosed PRQL block",
			input: "workflow main {\n  (from users\n}",
			expected: []TokenType{
				WORKFLOW, IDENT, LBRACE, NEWLINE,
				INDENT, PRQL_BLOCK, DEDENT, EOF,
			},
		},
		{
			name:  "malformed number (lonely sign)",
			input: "step x = + ",
			expected: []TokenType{
				STEP, IDENT, ASSIGN, ILLEGAL, EOF,
			},
		},
		{
			name:  "invalid comment start",
			input: "workflow / main",
			expected: []TokenType{
				WORKFLOW, ILLEGAL, IDENT, EOF,
			},
		},
		{
			name:  "unclosed string in import",
			input: "import \"std/io",
			expected: []TokenType{
				IMPORT, STRING_LIT, EOF,
			},
		},
		{
			name:  "illegal character in resource",
			input: "resource db # = postgres",
			expected: []TokenType{
				RESOURCE, IDENT, ILLEGAL, ASSIGN, IDENT, EOF,
			},
		},
		{
			name:  "illegal character in step",
			input: "step process $ = plugin.run",
			expected: []TokenType{
				STEP, IDENT, ILLEGAL, ASSIGN, IDENT, DOT, IDENT, EOF,
			},
		},
		{
			name:  "illegal character in handler",
			input: "handler on_error ! { }",
			expected: []TokenType{
				HANDLER, IDENT, ILLEGAL, LBRACE, RBRACE, EOF,
			},
		},

		// Layout and removed features
		{
			name:     "same-line pipe",
			input:    "step1 | step2",
			expected: []TokenType{IDENT, PIPE, IDENT},
		},
		{
			name:     "same-line assignment",
			input:    "step1 > output",
			expected: []TokenType{IDENT, RANGLE, IDENT},
		},
		{
			name:     "valid new-line pipe",
			input:    "step1\n| step2",
			expected: []TokenType{IDENT, NEWLINE, PIPE, IDENT},
		},
		{
			name:     "removed keywords",
			input:    "schema void timestamp bool ->",
			expected: []TokenType{IDENT, IDENT, IDENT, IDENT, ILLEGAL},
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

func TestDetailedIdentifiers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		expType TokenType
		expLit  string
	}{
		{"hyphenated", "my-step-name", IDENT, "my-step-name"},
		{"standalone prql", "(from users select id)", PRQL_BLOCK, "(from users select id)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := New(tt.input)
			tok := l.NextToken()
			if tok.Type != tt.expType {
				t.Fatalf("expected %v, got %v", tt.expType, tok.Type)
			}
			if tok.Literal != tt.expLit {
				t.Fatalf("expected %q, got %q", tt.expLit, tok.Literal)
			}
		})
	}
}
