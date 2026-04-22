package lexer

type TokenType string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	// Identifiers + literals
	IDENT      = "IDENT"
	STRING_LIT = "STRING_LIT"
	NUMBER_LIT = "NUMBER_LIT"
	PRQL_BLOCK = "PRQL_BLOCK"

	// Operators and punctuation
	ASSIGN   = "="
	COLON    = ":"
	LBRACE   = "{"
	RBRACE   = "}"
	LPAREN   = "("
	RPAREN   = ")"
	LBRACKET = "["
	RBRACKET = "]"
	ASTERISK = "*"
	RANGLE   = ">"
	LANGLE   = "<"
	PIPE     = "|"
	ARROW    = "->"
	QUESTION = "?"
	COMMA    = ","
	DOT      = "."

	// Keywords
	IMPORT    = "IMPORT"
	SCHEMA    = "SCHEMA"
	RESOURCE  = "RESOURCE"
	STEP      = "STEP"
	HANDLER   = "HANDLER"
	WORKFLOW  = "WORKFLOW"
	VOID      = "VOID"
	INT       = "INT"
	STRING    = "STRING"
	FLOAT     = "FLOAT"
	BOOL      = "BOOL"
	TIMESTAMP = "TIMESTAMP"
	TRUE      = "TRUE"
	FALSE     = "FALSE"
	NULL      = "NULL"

	// Formatting
	NEWLINE = "NEWLINE"
	INDENT  = "INDENT"
	DEDENT  = "DEDENT"
)

var keywords = map[string]TokenType{
	"import":    IMPORT,
	"schema":    SCHEMA,
	"resource":  RESOURCE,
	"step":      STEP,
	"handler":   HANDLER,
	"workflow":  WORKFLOW,
	"void":      VOID,
	"int":       INT,
	"string":    STRING,
	"float":     FLOAT,
	"bool":      BOOL,
	"timestamp": TIMESTAMP,
	"true":      TRUE,
	"false":     FALSE,
	"null":      NULL,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

func newToken(tokenType TokenType, literal string, line int, column int) Token {
	return Token{Type: tokenType, Literal: literal, Line: line, Column: column}
}
