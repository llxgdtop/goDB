package parser

import (
	"strings"
	"unicode"
)

// TokenType represents the type of a token
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError
	TokenIdentifier
	TokenString
	TokenNumber
	// Keywords
	TokenCREATE
	TokenTABLE
	TokenINSERT
	TokenINTO
	TokenVALUES
	TokenSELECT
	TokenFROM
	TokenWHERE
	TokenUPDATE
	TokenSET
	TokenDELETE
	TokenJOIN
	TokenINNER
	TokenLEFT
	TokenRIGHT
	TokenCROSS
	TokenON
	TokenAS
	TokenAND
	TokenOR
	TokenNOT
	TokenNULL
	TokenTRUE
	TokenFALSE
	TokenPRIMARY
	TokenKEY
	TokenDEFAULT
	TokenORDER
	TokenBY
	TokenASC
	TokenDESC
	TokenLIMIT
	TokenOFFSET
	TokenGROUP
	TokenHAVING
	TokenBEGIN
	TokenCOMMIT
	TokenROLLBACK
	TokenBOOLEAN
	TokenINTEGER
	TokenFLOAT
	TokenSTRING
	TokenCOUNT
	TokenMIN
	TokenMAX
	TokenSUM
	TokenAVG
	TokenSHOW
	TokenTABLES
	// Symbols
	TokenComma
	TokenSemicolon
	TokenLParen
	TokenRParen
	TokenStar
	TokenDot
	TokenEq
	TokenNe
	TokenLt
	TokenLe
	TokenGt
	TokenGe
	TokenPlus
	TokenMinus
	TokenMultiply
	TokenDivide
)

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// Lexer tokenizes SQL input
type Lexer struct {
	input   string
	pos     int
	lastPos int
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Pos: l.pos}
	}

	l.lastPos = l.pos
	ch := l.input[l.pos]

	switch {
	case ch == '\'':
		return l.readString()
	case ch == '"':
		return l.readQuotedIdentifier()
	case unicode.IsDigit(rune(ch)):
		return l.readNumber()
	case isLetter(ch):
		return l.readIdentifier()
	default:
		return l.readSymbol()
	}
}

// PeekToken peeks at the next token without consuming it
func (l *Lexer) PeekToken() Token {
	pos := l.pos
	token := l.NextToken()
	l.pos = pos
	return token
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
		} else {
			break
		}
	}
}

func (l *Lexer) readString() Token {
	l.pos++ // skip opening quote
	start := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				l.pos += 2 // escaped quote
				continue
			}
			value := l.input[start:l.pos]
			l.pos++ // skip closing quote
			return Token{Type: TokenString, Value: value, Pos: start}
		}
		l.pos++
	}

	return Token{Type: TokenError, Value: "unterminated string", Pos: start}
}

func (l *Lexer) readQuotedIdentifier() Token {
	l.pos++ // skip opening quote
	start := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '"' {
			value := l.input[start:l.pos]
			l.pos++ // skip closing quote
			return Token{Type: TokenIdentifier, Value: value, Pos: start}
		}
		l.pos++
	}

	return Token{Type: TokenError, Value: "unterminated quoted identifier", Pos: start}
}

func (l *Lexer) readNumber() Token {
	start := l.pos
	hasDot := false

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsDigit(rune(ch)) {
			l.pos++
		} else if ch == '.' && !hasDot {
			hasDot = true
			l.pos++
		} else {
			break
		}
	}

	return Token{Type: TokenNumber, Value: l.input[start:l.pos], Pos: start}
}

func (l *Lexer) readIdentifier() Token {
	start := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if isLetter(ch) || unicode.IsDigit(rune(ch)) || ch == '_' {
			l.pos++
		} else {
			break
		}
	}

	value := l.input[start:l.pos]
	tokenType := lookupKeyword(value)
	return Token{Type: tokenType, Value: value, Pos: start}
}

func (l *Lexer) readSymbol() Token {
	ch := l.input[l.pos]
	l.pos++

	switch ch {
	case ',':
		return Token{Type: TokenComma, Pos: l.pos - 1}
	case ';':
		return Token{Type: TokenSemicolon, Pos: l.pos - 1}
	case '(':
		return Token{Type: TokenLParen, Pos: l.pos - 1}
	case ')':
		return Token{Type: TokenRParen, Pos: l.pos - 1}
	case '*':
		return Token{Type: TokenStar, Pos: l.pos - 1}
	case '.':
		return Token{Type: TokenDot, Pos: l.pos - 1}
	case '+':
		return Token{Type: TokenPlus, Pos: l.pos - 1}
	case '-':
		return Token{Type: TokenMinus, Pos: l.pos - 1}
	case '/':
		return Token{Type: TokenDivide, Pos: l.pos - 1}
	case '=':
		return Token{Type: TokenEq, Pos: l.pos - 1}
	case '<':
		if l.pos < len(l.input) {
			if l.input[l.pos] == '=' {
				l.pos++
				return Token{Type: TokenLe, Pos: l.pos - 2}
			}
			if l.input[l.pos] == '>' {
				l.pos++
				return Token{Type: TokenNe, Pos: l.pos - 2}
			}
		}
		return Token{Type: TokenLt, Pos: l.pos - 1}
	case '>':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokenGe, Pos: l.pos - 2}
		}
		return Token{Type: TokenGt, Pos: l.pos - 1}
	case '!':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: TokenNe, Pos: l.pos - 2}
		}
		return Token{Type: TokenError, Value: "unexpected !", Pos: l.pos - 1}
	default:
		return Token{Type: TokenError, Value: string(ch), Pos: l.pos - 1}
	}
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func lookupKeyword(s string) TokenType {
	switch strings.ToUpper(s) {
	case "CREATE":
		return TokenCREATE
	case "TABLE":
		return TokenTABLE
	case "INSERT":
		return TokenINSERT
	case "INTO":
		return TokenINTO
	case "VALUES":
		return TokenVALUES
	case "SELECT":
		return TokenSELECT
	case "FROM":
		return TokenFROM
	case "WHERE":
		return TokenWHERE
	case "UPDATE":
		return TokenUPDATE
	case "SET":
		return TokenSET
	case "DELETE":
		return TokenDELETE
	case "JOIN":
		return TokenJOIN
	case "INNER":
		return TokenINNER
	case "LEFT":
		return TokenLEFT
	case "RIGHT":
		return TokenRIGHT
	case "CROSS":
		return TokenCROSS
	case "ON":
		return TokenON
	case "AS":
		return TokenAS
	case "AND":
		return TokenAND
	case "OR":
		return TokenOR
	case "NOT":
		return TokenNOT
	case "NULL":
		return TokenNULL
	case "TRUE":
		return TokenTRUE
	case "FALSE":
		return TokenFALSE
	case "PRIMARY":
		return TokenPRIMARY
	case "KEY":
		return TokenKEY
	case "DEFAULT":
		return TokenDEFAULT
	case "ORDER":
		return TokenORDER
	case "BY":
		return TokenBY
	case "ASC":
		return TokenASC
	case "DESC":
		return TokenDESC
	case "LIMIT":
		return TokenLIMIT
	case "OFFSET":
		return TokenOFFSET
	case "GROUP":
		return TokenGROUP
	case "HAVING":
		return TokenHAVING
	case "BEGIN":
		return TokenBEGIN
	case "COMMIT":
		return TokenCOMMIT
	case "ROLLBACK":
		return TokenROLLBACK
	case "BOOLEAN", "BOOL":
		return TokenBOOLEAN
	case "INTEGER", "INT":
		return TokenINTEGER
	case "FLOAT", "DOUBLE", "REAL":
		return TokenFLOAT
	case "STRING", "TEXT", "VARCHAR":
		return TokenSTRING
	case "COUNT":
		return TokenCOUNT
	case "MIN":
		return TokenMIN
	case "MAX":
		return TokenMAX
	case "SUM":
		return TokenSUM
	case "AVG":
		return TokenAVG
	case "SHOW":
		return TokenSHOW
	case "TABLES":
		return TokenTABLES
	default:
		return TokenIdentifier
	}
}
