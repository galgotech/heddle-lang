package lexer

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
	line         int
	column       int

	indentStack []int
	pending     []Token
	tabSize     int
	indentStyle rune
	lastTokType TokenType
}

func New(input string) *Lexer {
	l := &Lexer{
		input:       input,
		line:        1,
		column:      0,
		indentStack: []int{0},
		tabSize:     4,
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
		l.lastTokType = tok.Type
		return tok
	}

	l.skipInlineWhitespace()

	startLine := l.line
	startCol := l.column

	var tok Token

	switch l.ch {
	case '\n', '\r':
		tok = l.handleNewLine()
		l.lastTokType = tok.Type
		return tok
	case '=':
		tok = newToken(ASSIGN, string(l.ch), startLine, startCol, l.line, l.column+1)
	case ':':
		tok = newToken(COLON, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '{':
		tok = newToken(LBRACE, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '}':
		tok = newToken(RBRACE, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '(':
		tok.Type = PRQL_BLOCK
		tok.Literal = l.readPRQLBlock()
		tok.Line = startLine
		tok.Column = startCol
		tok.EndLine = l.line
		tok.EndColumn = l.column
		l.lastTokType = tok.Type
		return tok
	case ')':
		tok = newToken(RPAREN, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '[':
		tok = newToken(LBRACKET, string(l.ch), startLine, startCol, l.line, l.column+1)
	case ']':
		tok = newToken(RBRACKET, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '*':
		tok = newToken(ASTERISK, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '>':
		tok = newToken(RANGLE, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '<':
		tok = newToken(LANGLE, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '|':
		tok = newToken(PIPE, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '?':
		tok = newToken(QUESTION, string(l.ch), startLine, startCol, l.line, l.column+1)
	case ',':
		tok = newToken(COMMA, string(l.ch), startLine, startCol, l.line, l.column+1)
	case '.':
		tok = newToken(DOT, string(l.ch), startLine, startCol, l.line, l.column+1)
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
		l.lastTokType = tok.Type
		return tok
	case '/':
		if l.peekChar() == '/' {
			l.skipComment()
			return l.NextToken()
		}
		tok = newToken(ILLEGAL, string(l.ch), startLine, startCol, l.line, l.column+1)
	case 0:
		if len(l.indentStack) > 1 {
			l.indentStack = l.indentStack[:len(l.indentStack)-1]
			tok = newToken(DEDENT, "", l.line, l.column, l.line, l.column)
			l.lastTokType = tok.Type
			return tok
		}
		tok.Type = EOF
		tok.Literal = ""
		tok.Line = l.line
		tok.Column = l.column
		tok.EndLine = l.line
		tok.EndColumn = l.column
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			tok.Line = startLine
			tok.Column = startCol
			tok.EndLine = l.line
			tok.EndColumn = l.column
			l.lastTokType = tok.Type
			return tok
		} else if isDigit(l.ch) {
			tok = l.readNumber()
			l.lastTokType = tok.Type
			return tok
		} else {
			tok = newToken(ILLEGAL, string(l.ch), startLine, startCol, l.line, l.column+1)
		}
	}

	l.readChar()
	l.lastTokType = tok.Type
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
	literal := ""

	for l.ch == '\n' || l.ch == '\r' {
		literal += string(l.ch)
		l.readChar()
		l.line++
		l.column = 0
	}

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

	if hasSpace && hasTab {
		l.pending = append(l.pending, newToken(ILLEGAL, "mixed tabs and spaces in indentation", l.line, 1, l.line, 1))
	}

	if len(indentation) > 0 {
		currentStyle := rune(indentation[0])
		if l.indentStyle == 0 {
			l.indentStyle = currentStyle
		} else if l.indentStyle != currentStyle {
			l.pending = append(l.pending, newToken(ILLEGAL, "conflicting indentation style in file", l.line, 1, l.line, 1))
		}
	}

	if l.ch == '\n' || l.ch == '\r' || l.ch == 0 || (l.ch == '/' && l.peekChar() == '/') {
		return l.NextToken()
	}

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
		l.indentStack = append(l.indentStack, currentIndent)
		l.pending = append(l.pending, newToken(INDENT, indentation, l.line, 1, l.line, len(indentation)+1))
	} else if currentIndent < lastIndent {
		for currentIndent < lastIndent {
			l.indentStack = l.indentStack[:len(l.indentStack)-1]
			l.pending = append(l.pending, newToken(DEDENT, "", l.line, 1, l.line, 1))
			if len(l.indentStack) == 0 {
				break
			}
			lastIndent = l.indentStack[len(l.indentStack)-1]
		}
		if currentIndent != lastIndent {
			l.pending = append(l.pending, newToken(ILLEGAL, "unindent does not match any outer indentation level", l.line, 1, l.line, 1))
		}
	}

	hasDedent := false
	for _, t := range l.pending {
		if t.Type == DEDENT {
			hasDedent = true
			break
		}
	}

	nlTok := newToken(NEWLINE, literal, line, col, l.line, 1)
	if hasDedent {
		l.pending = append(l.pending, nlTok)
		tok := l.pending[0]
		l.pending = l.pending[1:]
		l.lastTokType = tok.Type
		return tok
	}

	l.lastTokType = nlTok.Type
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
	if !isDigit(l.ch) && l.ch != '.' {
		return Token{
			Type:      ILLEGAL,
			Literal:   l.input[position:l.position],
			Line:      line,
			Column:    col,
			EndLine:   l.line,
			EndColumn: l.column,
		}
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
	return Token{
		Type:      NUMBER_LIT,
		Literal:   l.input[position:l.position],
		Line:      line,
		Column:    col,
		EndLine:   l.line,
		EndColumn: l.column,
	}
}

func (l *Lexer) readEscapedString() string {
	l.readChar()
	start := l.position
	for l.ch != '"' && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar()
		}
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar()
	return literal
}

func (l *Lexer) readPRQLBlock() string {
	start := l.position
	depth := 1
	l.readChar()
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
		} else if l.ch == '\n' || l.ch == '\r' {
			l.line++
			l.column = 0
		}
		if depth > 0 {
			l.readChar()
		}
	}
	literal := l.input[start : l.position+1]
	l.readChar()
	return literal
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
