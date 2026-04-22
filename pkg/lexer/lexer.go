package lexer

type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
	line         int
	column       int

	indentStack []int
	pending     []Token
	tabSize     int
	indentStyle rune // ' ' for spaces, '\t' for tabs, 0 for not yet determined
}

func New(input string) *Lexer {
	l := &Lexer{
		input:       input,
		line:        1,
		column:      0,
		indentStack: []int{0},
		tabSize:     4, // Default tab size
		indentStyle: 0,
	}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	l.column++
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

func (l *Lexer) NextToken() Token {
	if len(l.pending) > 0 {
		tok := l.pending[0]
		l.pending = l.pending[1:]
		return tok
	}

	l.skipInlineWhitespace()

	var tok Token

	switch l.ch {
	case '\n', '\r':
		return l.handleNewLine()
	case '=':
		tok = newToken(ASSIGN, string(l.ch), l.line, l.column)
	case ':':
		tok = newToken(COLON, string(l.ch), l.line, l.column)
	case '{':
		tok = newToken(LBRACE, string(l.ch), l.line, l.column)
	case '}':
		tok = newToken(RBRACE, string(l.ch), l.line, l.column)
	case '[':
		tok = newToken(LBRACKET, string(l.ch), l.line, l.column)
	case ']':
		tok = newToken(RBRACKET, string(l.ch), l.line, l.column)
	case '*':
		tok = newToken(ASTERISK, string(l.ch), l.line, l.column)
	case '>':
		tok = newToken(RANGLE, string(l.ch), l.line, l.column)
	case '<':
		tok = newToken(LANGLE, string(l.ch), l.line, l.column)
	case '|':
		tok = newToken(PIPE, string(l.ch), l.line, l.column)
	case '?':
		tok = newToken(QUESTION, string(l.ch), l.line, l.column)
	case ',':
		tok = newToken(COMMA, string(l.ch), l.line, l.column)
	case '.':
		tok = newToken(DOT, string(l.ch), l.line, l.column)
	case '-':
		if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = newToken(ARROW, string(ch)+string(l.ch), l.line, l.column-1)
		} else {
			tok = l.readNumber()
			return tok
		}
	case '"':
		tok.Type = STRING_LIT
		tok.Literal = l.readEscapedString()
		tok.Line = l.line
		tok.Column = l.column
		return tok
	case '(':
		tok.Type = PRQL_BLOCK
		tok.Literal = l.readPRQLBlock()
		tok.Line = l.line
		tok.Column = l.column
		return tok
	case '/':
		if l.peekChar() == '/' {
			l.skipComment()
			return l.NextToken()
		}
		tok = newToken(ILLEGAL, string(l.ch), l.line, l.column)
	case 0:
		if len(l.indentStack) > 1 {
			l.indentStack = l.indentStack[:len(l.indentStack)-1]
			return newToken(DEDENT, "", l.line, l.column)
		}
		tok.Type = EOF
		tok.Literal = ""
		tok.Line = l.line
		tok.Column = l.column
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			tok.Line = l.line
			tok.Column = l.column
			return tok
		} else if isDigit(l.ch) {
			tok = l.readNumber()
			return tok
		} else {
			tok = newToken(ILLEGAL, string(l.ch), l.line, l.column)
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipInlineWhitespace() {
	for l.ch == ' ' || l.ch == '\t' {
		l.readChar()
	}
}

func (l *Lexer) handleNewLine() Token {
	line := l.line
	col := l.column
	literal := "\n"
	if l.ch == '\r' && l.peekChar() == '\n' {
		l.readChar()
		literal = "\r\n"
	}
	l.readChar()
	l.line++
	l.column = 0

	// Peek ahead for indentation of next non-empty line
	indentation := ""
	tempPos := l.position
	tempReadPos := l.readPosition
	tempCh := l.ch
	tempLine := l.line
	tempCol := l.column

	for {
		if l.ch == ' ' || l.ch == '\t' {
			indentation += string(l.ch)
			l.readChar()
		} else if l.ch == '\n' || l.ch == '\r' {
			// Empty line, ignore its indentation
			if l.ch == '\r' && l.peekChar() == '\n' {
				l.readChar()
			}
			l.readChar()
			l.line++
			l.column = 0
			indentation = ""
		} else if l.ch == '/' && l.peekChar() == '/' {
			// Comment line, ignore its indentation
			for l.ch != '\n' && l.ch != '\r' && l.ch != 0 {
				l.readChar()
			}
			// Continue to next line
		} else {
			break
		}
	}

	if l.ch != 0 {
		// Check for mixed indentation
		hasSpaces := false
		hasTabs := false
		for _, char := range indentation {
			if char == ' ' {
				hasSpaces = true
			} else if char == '\t' {
				hasTabs = true
			}
		}

		if hasSpaces && hasTabs {
			l.pending = append(l.pending, newToken(ILLEGAL, "mixed tabs and spaces in indentation", l.line, 1))
		} else {
			var currentStyle rune
			if hasSpaces {
				currentStyle = ' '
			} else if hasTabs {
				currentStyle = '\t'
			}

			if currentStyle != 0 {
				if l.indentStyle == 0 {
					l.indentStyle = currentStyle
				} else if l.indentStyle != currentStyle {
					l.pending = append(l.pending, newToken(ILLEGAL, "inconsistent indentation style (tabs vs spaces)", l.line, 1))
				}
			}

			currentIndent := l.calculateIndent(indentation)
			lastIndent := l.indentStack[len(l.indentStack)-1]

			if currentIndent > lastIndent {
				l.indentStack = append(l.indentStack, currentIndent)
				l.pending = append(l.pending, newToken(INDENT, indentation, l.line, 1))
			} else if currentIndent < lastIndent {
				for currentIndent < lastIndent {
					l.indentStack = l.indentStack[:len(l.indentStack)-1]
					l.pending = append(l.pending, newToken(DEDENT, "", l.line, 1))
					if len(l.indentStack) == 0 {
						break
					}
					lastIndent = l.indentStack[len(l.indentStack)-1]
				}
				if currentIndent != lastIndent {
					l.pending = append(l.pending, newToken(ILLEGAL, "unindent does not match any outer indentation level", l.line, 1))
				}
			}
		}
	}

	// Restore position after peeking
	l.position = tempPos
	l.readPosition = tempReadPos
	l.ch = tempCh
	l.line = tempLine
	l.column = tempCol

	// If we have pending DEDENTs, they should come BEFORE the NEWLINE
	// that triggered them, to match the grammar pattern _DEDENT _NL "}"
	hasDedent := false
	for _, tok := range l.pending {
		if tok.Type == DEDENT {
			hasDedent = true
			break
		}
	}

	nlTok := newToken(NEWLINE, literal, line, col)
	if hasDedent {
		// Put the NEWLINE after the DEDENTs in the pending queue
		l.pending = append(l.pending, nlTok)
		// Return the first DEDENT
		tok := l.pending[0]
		l.pending = l.pending[1:]
		return tok
	}

	return nlTok
}

func (l *Lexer) skipComment() {
	for l.ch != '\n' && l.ch != '\r' && l.ch != 0 {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '-' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() Token {
	line := l.line
	col := l.column
	position := l.position
	if l.ch == '-' || l.ch == '+' {
		l.readChar()
	}
	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return Token{Type: NUMBER_LIT, Literal: l.input[position:l.position], Line: line, Column: col}
}

func (l *Lexer) readEscapedString() string {
	l.readChar() // consume "
	start := l.position
	for l.ch != '"' && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar()
		}
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar() // consume "
	return literal
}

func (l *Lexer) readPRQLBlock() string {
	start := l.position
	depth := 1
	l.readChar() // consume (
	for depth > 0 && l.ch != 0 {
		if l.ch == '(' {
			depth++
		} else if l.ch == ')' {
			depth--
		} else if l.ch == '"' || l.ch == '\'' {
			quote := l.ch
			l.readChar()
			for l.ch != quote && l.ch != 0 {
				if l.ch == '\\' {
					l.readChar()
				}
				l.readChar()
			}
		}
		l.readChar()
	}
	return l.input[start:l.position]
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) calculateIndent(indentation string) int {
	width := 0
	for _, char := range indentation {
		if char == '\t' {
			width += l.tabSize - (width % l.tabSize)
		} else {
			width++
		}
	}
	return width
}
