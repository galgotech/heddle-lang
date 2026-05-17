// Package lexer implements the lexical analyzer for the Heddle Domain Specific Language.
// It converts source code into a stream of tokens, handling complex rules for
// indentation-based scoping and embedded PRQL expressions.
package lexer

import (
	"strings"
)

// isLetter returns true if the character is an ASCII letter or underscore.
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '-'
}

// isDigit returns true if the character is an ASCII decimal digit.
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// Lexer performs lexical analysis on Heddle source code, converting raw input
// into a stream of tokens while tracking indentation levels for block scoping.
type Lexer struct {
	input        string // the entire source code being lexed.
	line         int    // current line number (1-indexed)
	readPosition int    // current reading position in input (after current char)
	column       int    // current column number (0-indexed)
	position     int    // current position in input (points to current char)
	ch           byte   // current char under examination

	indentStack []int     // stack of indentation levels (measured in spaces)
	pending     []Token   // queue for tokens generated during lookahead (e.g., INDENT/DEDENT)
	tabSize     int       // number of spaces a tab represents
	indentStyle rune      // ' ' or '\t', enforced once detected
	lastTokType TokenType // type of the most recently emitted token
}

// NextToken returns the next recognized token from the input. It prioritizes
// tokens in the pending queue (e.g., INDENT/DEDENT) before scanning new characters.
func (l *Lexer) NextToken() Token {
	if len(l.pending) > 0 {
		tok := l.pending[0]
		l.pending = l.pending[1:]
		l.lastTokType = tok.Type
		return tok
	}

	l.skipInlineWhitespace()

	startLine := l.line
	startCol := l.column
	startPos := l.position

	var tok Token

	switch l.ch {
	case '\n', '\r':
		tok = l.handleNewLine()
		l.lastTokType = tok.Type
		return tok
	case '=':
		tok = newToken(ASSIGN, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case ':':
		tok = newToken(COLON, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '{':
		tok = newToken(LBRACE, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '}':
		tok = newToken(RBRACE, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '(':
		// Parentheses delimit PRQL blocks in Heddle DSL
		tok.Type = PRQL_BLOCK
		tok.Literal = l.readPRQLBlock()
		tok.Line = startLine
		tok.Column = startCol
		tok.EndLine = l.line
		tok.EndColumn = l.column
		tok.Start = startPos
		l.lastTokType = tok.Type
		return tok
	case ')':
		tok = newToken(RPAREN, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '[':
		tok = newToken(LBRACKET, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case ']':
		tok = newToken(RBRACKET, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '*':
		tok = newToken(ASTERISK, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '>':
		tok = newToken(RANGLE, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '<':
		tok = newToken(LANGLE, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '|':
		tok = newToken(PIPE, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '?':
		tok = newToken(QUESTION, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case ',':
		tok = newToken(COMMA, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '+':
		tok = l.readNumber()
		l.lastTokType = tok.Type
		return tok
	case '.':
		if isDigit(l.peekChar()) {
			tok = l.readNumber()
			l.lastTokType = tok.Type
			return tok
		}
		tok = newToken(DOT, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case '-':
		tok = l.readNumber()
		l.lastTokType = tok.Type
		return tok
	case '"':
		tok.Type = STRING_LIT
		tok.Literal = l.readEscapedString()
		tok.Line = startLine
		tok.Column = startCol
		tok.EndLine = l.line
		tok.EndColumn = l.column
		tok.Start = startPos
		l.lastTokType = tok.Type
		return tok
	case '/':
		if l.peekChar() == '/' {
			l.skipComment()
			return l.NextToken()
		}
		if l.peekChar() == '*' {
			tok.Type = BLOCK_COMMENT
			tok.Literal = l.readBlockComment()
			tok.Line = startLine
			tok.Column = startCol
			tok.EndLine = l.line
			tok.EndColumn = l.column
			tok.Start = startPos
			l.lastTokType = tok.Type
			return tok
		}
		tok = newToken(ILLEGAL, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
	case 0:
		// Check for missing DEDENT tokens at end of file
		if len(l.indentStack) > 1 {
			l.indentStack = l.indentStack[:len(l.indentStack)-1]
			tok = newToken(DEDENT, "", l.line, l.column, l.line, l.column, l.position)
			l.lastTokType = tok.Type
			return tok
		}
		tok.Type = EOF
		tok.Literal = ""
		tok.Line = l.line
		tok.Column = l.column
		tok.EndLine = l.line
		tok.EndColumn = l.column
		tok.Start = l.position
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			tok.Line = startLine
			tok.Column = startCol
			tok.EndLine = l.line
			tok.EndColumn = l.column
			tok.Start = startPos
			l.lastTokType = tok.Type
			return tok
		} else if isDigit(l.ch) {
			tok = l.readNumber()
			l.lastTokType = tok.Type
			return tok
		} else {
			tok = newToken(ILLEGAL, string(l.ch), startLine, startCol, l.line, l.column+1, startPos)
		}
	}

	l.readChar()
	l.lastTokType = tok.Type
	return tok
}

// skipInlineWhitespace consumes spaces and tabs that do not affect indentation levels.
func (l *Lexer) skipInlineWhitespace() {
	for l.ch == ' ' || l.ch == '\t' {
		l.readChar()
	}
}

// handleNewLine processes line breaks and calculates indentation changes.
// It emits INDENT/DEDENT tokens to represent block-level scoping and
// enforces consistent indentation styles within a file.
func (l *Lexer) handleNewLine() Token {
	line := l.line
	col := l.column
	literal := ""

	// Consume all consecutive line break characters
	for l.ch == '\n' || l.ch == '\r' {
		literal += string(l.ch)
		l.readChar()
		l.line++
		l.column = 1
	}

	// Capture leading whitespace on the new line
	indentation := ""
	hasSpace := false
	hasTab := false
	for l.ch == ' ' || l.ch == '\t' {
		if l.ch == ' ' {
			hasSpace = true
		} else if l.ch == '\t' {
			hasTab = true
		}
		indentation += string(l.ch)
		l.readChar()
	}

	// Validate indentation consistency (mixing tabs and spaces is disallowed)
	if hasSpace && hasTab {
		l.pending = append(l.pending, newToken(ILLEGAL, "mixed tabs and spaces in indentation", l.line, 1, l.line, 1, l.position-len(indentation)))
	}

	// Enforce a single indentation style (all tabs or all spaces) for the entire file
	if len(indentation) > 0 {
		currentStyle := rune(indentation[0])
		if l.indentStyle == 0 {
			l.indentStyle = currentStyle
		} else if l.indentStyle != currentStyle {
			l.pending = append(l.pending, newToken(ILLEGAL, "conflicting indentation style in file", l.line, 1, l.line, 1, l.position-len(indentation)))
		}
	}

	// Ignore indentation for empty lines, whitespace-only lines, or comment lines
	if l.ch == '\n' || l.ch == '\r' || l.ch == 0 || (l.ch == '/' && l.peekChar() == '/') {
		return l.NextToken()
	}

	// Calculate the numeric indentation level
	currentIndent := 0
	for _, char := range indentation {
		if char == ' ' {
			currentIndent++
		} else if char == '\t' {
			currentIndent += l.tabSize
		}
	}

	lastIndent := l.indentStack[len(l.indentStack)-1]

	if currentIndent > lastIndent {
		// New indentation level detected: emit one or more INDENT tokens
		diff := currentIndent - lastIndent
		if diff%l.tabSize != 0 {
			// If not a multiple, we still allow it but just emit one INDENT for this specific level.
			// However, Heddle usually enforces multiples. Let's be lenient but consistent with the stack.
			l.indentStack = append(l.indentStack, currentIndent)
			l.pending = append(l.pending, newToken(INDENT, indentation, l.line, 1, l.line, len(indentation)+1, l.position-len(indentation)))
		} else {
			for i := 0; i < diff/l.tabSize; i++ {
				lastIndent += l.tabSize
				l.indentStack = append(l.indentStack, lastIndent)
				l.pending = append(l.pending, newToken(INDENT, indentation, l.line, 1, l.line, 1, l.position-len(indentation)))
			}
		}
	} else if currentIndent < lastIndent {
		// Decreased indentation level: emit DEDENT(s) until matching an outer level
		for currentIndent < lastIndent {
			l.indentStack = l.indentStack[:len(l.indentStack)-1]
			l.pending = append(l.pending, newToken(DEDENT, "", l.line, 1, l.line, 1, l.position))
			if len(l.indentStack) == 0 {
				break
			}
			lastIndent = l.indentStack[len(l.indentStack)-1]
		}
		// Error if the unindent doesn't align with any previous level
		if currentIndent != lastIndent {
			l.pending = append(l.pending, newToken(ILLEGAL, "unindent does not match any outer indentation level", l.line, 1, l.line, 1, l.position))
		}
	}

	hasDedent := false
	for _, t := range l.pending {
		if t.Type == DEDENT {
			hasDedent = true
			break
		}
	}

	nlTok := newToken(NEWLINE, literal, line, col, l.line, 1, l.position-len(literal)-len(indentation))
	if hasDedent {
		// If DEDENTs were generated, they must be emitted before the NEWLINE
		l.pending = append(l.pending, nlTok)
		tok := l.pending[0]
		l.pending = l.pending[1:]
		l.lastTokType = tok.Type
		return tok
	}

	l.lastTokType = nlTok.Type
	return nlTok
}

// skipComment consumes all characters until the end of the current line.
func (l *Lexer) skipComment() {
	for l.ch != '\n' && l.ch != '\r' && l.ch != 0 {
		l.readChar()
	}
}

// readBlockComment captures a comment block starting with '/*' and ending with '*/'.
func (l *Lexer) readBlockComment() string {
	start := l.position
	l.readChar() // Skip '/'
	l.readChar() // Skip '*'

	for l.ch != 0 {
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar() // Skip '*'
			l.readChar() // Skip '/'
			break
		}

		if l.ch == '\n' || l.ch == '\r' {
			if l.readPosition < len(l.input) {
				rem := l.input[l.readPosition:]
				if strings.HasPrefix(rem, "workflow ") ||
					strings.HasPrefix(rem, "step ") ||
					strings.HasPrefix(rem, "resource ") ||
					strings.HasPrefix(rem, "import ") ||
					strings.HasPrefix(rem, "handler ") {
					break
				}
			}
			l.line++
			l.column = 1
		}
		l.readChar()
	}

	return l.input[start:l.position]
}

// readIdentifier captures an alphanumeric identifier or keyword.
// It also supports hyphens to allow for kebab-case identifiers.
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '-' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber captures numeric literals, distinguishing between INT and FLOAT types.
func (l *Lexer) readNumber() Token {
	line := l.line
	col := l.column
	position := l.position
	isFloat := false

	if l.ch == '-' || l.ch == '+' {
		l.readChar()
	}

	if !isDigit(l.ch) && l.ch != '.' {
		return Token{
			Type:      ILLEGAL,
			Literal:   l.input[position:l.position],
			Line:      line,
			Column:    col,
			EndLine:   l.line,
			EndColumn: l.column,
			Start:     position,
		}
	}

	for isDigit(l.ch) {
		l.readChar()
	}

	if l.ch == '.' {
		isFloat = true
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	var tokenType TokenType = INT
	if isFloat {
		tokenType = FLOAT
	}

	return Token{
		Type:      tokenType,
		Literal:   l.input[position:l.position],
		Line:      line,
		Column:    col,
		EndLine:   l.line,
		EndColumn: l.column,
		Start:     position,
	}
}

// readEscapedString captures a double-quoted string literal,
// correctly handling backslash-escaped characters.
func (l *Lexer) readEscapedString() string {
	l.readChar() // Skip the opening quote
	start := l.position
	for l.ch != '"' && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar() // Skip the escape character
		}
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar() // Skip the closing quote
	return literal
}

// readPRQLBlock captures a PRQL expression enclosed in parentheses.
// It tracks nesting depth and ignores content within strings to ensure
// it correctly identifies the terminating closing parenthesis.
func (l *Lexer) readPRQLBlock() string {
	start := l.position
	depth := 1
	l.readChar() // Skip opening parenthesis
	for depth > 0 && l.ch != 0 {
		if l.ch == '(' {
			depth++
		} else if l.ch == ')' {
			depth--
		} else if l.ch == '"' || l.ch == '\'' {
			// Handle strings inside PRQL blocks to avoid miscounting parentheses
			quote := l.ch
			l.readChar()
			for l.ch != quote && l.ch != 0 {
				if l.ch == '\\' {
					l.readChar()
				}
				l.readChar()
			}
		} else if l.ch == '\n' || l.ch == '\r' {
			l.line++
			l.column = 1
		}
		if depth > 0 {
			l.readChar()
		}
	}
	// Extract the block including the parentheses
	endPos := min(l.position+1, len(l.input))
	literal := l.input[start:endPos]
	l.readChar() // Move past the closing parenthesis (or EOF)
	return literal
}

// readChar advances the lexer to the next character in the input string
// and updates the current position tracking.
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	l.column++
}

// peekChar returns the next character in the input without advancing the lexer's position.
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// New initializes a Lexer with the provided input string and default configuration.
func New(input string) *Lexer {
	l := &Lexer{
		input:       input,
		line:        1,
		column:      0,
		indentStack: []int{0},
		tabSize:     2,
		indentStyle: 0,
	}
	l.readChar() // Load the first character
	return l
}
