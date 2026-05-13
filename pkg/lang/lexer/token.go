package lexer

type TokenType string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	// Identifiers + literals
	IDENTIFIER = "IDENTIFIER"
	STRING_LIT = "STRING_LIT"
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
	QUESTION = "?"
	COMMA    = ","
	DOT      = "."

	// Keywords
	IMPORT   = "IMPORT"
	RESOURCE = "RESOURCE"
	STEP     = "STEP"
	HANDLER  = "HANDLER"
	WORKFLOW = "WORKFLOW"
	INT      = "INT"
	STRING   = "STRING"
	FLOAT    = "FLOAT"
	BOOLEAN  = "BOOLEAN"
	TRUE     = "TRUE"
	FALSE    = "FALSE"
	NULL     = "NULL"

	// Formatting
	NEWLINE = "NEWLINE"
	INDENT  = "INDENT"
	DEDENT  = "DEDENT"

	// Comments
	BLOCK_COMMENT = "BLOCK_COMMENT"
)

var keywords = map[string]TokenType{
	"import":   IMPORT,
	"resource": RESOURCE,
	"step":     STEP,
	"handler":  HANDLER,
	"workflow": WORKFLOW,
	"int":      INT,
	"string":   STRING,
	"float":    FLOAT,
	"boolean":  BOOLEAN,
	"true":     TRUE,
	"false":    FALSE,
	"null":     NULL,
}

type Token struct {
	Type      TokenType
	Literal   string
	Line      int
	Column    int
	EndLine   int
	EndColumn int
	Start     int
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENTIFIER
}

func newToken(tokenType TokenType, literal string, line, col, endLine, endCol, start int) Token {
	return Token{
		Type:      tokenType,
		Literal:   literal,
		Line:      line,
		Column:    col,
		EndLine:   endLine,
		EndColumn: endCol,
		Start:     start,
	}
}
