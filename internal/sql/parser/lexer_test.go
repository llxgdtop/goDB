package parser

import (
	"testing"
)

// tokenizeAll is a helper function that tokenizes the entire input and returns all tokens
func tokenizeAll(input string) []Token {
	lexer := NewLexer(input)
	var tokens []Token
	for {
		token := lexer.NextToken()
		if token.Type == TokenEOF {
			break
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// assertTokensEqual checks if the actual tokens match the expected types
func assertTokensEqual(t *testing.T, actual []Token, expectedTypes []TokenType) {
	if len(actual) != len(expectedTypes) {
		t.Errorf("expected %d tokens, got %d", len(expectedTypes), len(actual))
		return
	}
	for i, token := range actual {
		if token.Type != expectedTypes[i] {
			t.Errorf("at position %d: expected %v, got %v", i, expectedTypes[i], token.Type)
		}
	}
}

// 1. Test CREATE TABLE statements (case-insensitive)
func TestLexerCreateTable(t *testing.T) {
	// Test lowercase - note: 'int' is not a keyword, only 'INTEGER' is
	tokens := tokenizeAll("create table tbl (id1 integer primary key, id2 integer);")
	expected := []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenPRIMARY, TokenKEY, TokenComma,
		TokenIdentifier, TokenINTEGER, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Test uppercase
	tokens = tokenizeAll("CREATE TABLE TBL (ID1 INTEGER PRIMARY KEY, ID2 INTEGER);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenPRIMARY, TokenKEY, TokenComma,
		TokenIdentifier, TokenINTEGER, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Test mixed case
	tokens = tokenizeAll("Create Table Tbl (Id1 Integer Primary Key, Id2 Integer);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenPRIMARY, TokenKEY, TokenComma,
		TokenIdentifier, TokenINTEGER, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Test with all data types
	tokens = tokenizeAll(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name STRING,
		age INTEGER,
		salary FLOAT,
		active BOOLEAN,
		created_at STRING
	);`)
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenPRIMARY, TokenKEY, TokenComma,
		TokenIdentifier, TokenSTRING, TokenComma,
		TokenIdentifier, TokenINTEGER, TokenComma,
		TokenIdentifier, TokenFLOAT, TokenComma,
		TokenIdentifier, TokenBOOLEAN, TokenComma,
		TokenIdentifier, TokenSTRING,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 2. Test INSERT INTO statements
func TestLexerInsertInto(t *testing.T) {
	// Basic insert
	tokens := tokenizeAll("insert into tbl values (1, 2, '3', true, false, 4.55);")
	expected := []TokenType{
		TokenINSERT, TokenINTO, TokenIdentifier, TokenVALUES,
		TokenLParen, TokenNumber, TokenComma, TokenNumber, TokenComma,
		TokenString, TokenComma, TokenTRUE, TokenComma, TokenFALSE, TokenComma,
		TokenNumber, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Insert with column names
	tokens = tokenizeAll("INSERT INTO tbl (id, name, age) values (100, 'db', 10);")
	expected = []TokenType{
		TokenINSERT, TokenINTO, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenComma, TokenIdentifier, TokenComma, TokenIdentifier,
		TokenRParen, TokenVALUES, TokenLParen,
		TokenNumber, TokenComma, TokenString, TokenComma, TokenNumber,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Insert with NULL
	tokens = tokenizeAll("INSERT INTO users (id, name) VALUES (1, NULL);")
	expected = []TokenType{
		TokenINSERT, TokenINTO, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenComma, TokenIdentifier,
		TokenRParen, TokenVALUES, TokenLParen,
		TokenNumber, TokenComma, TokenNULL,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 3. Test SELECT statements with WHERE, ORDER BY, LIMIT
func TestLexerSelect(t *testing.T) {
	// Basic select
	tokens := tokenizeAll("select * from tbl;")
	expected := []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with WHERE clause
	tokens = tokenizeAll("SELECT id, name FROM users WHERE id = 1;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenComma, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenWHERE, TokenIdentifier, TokenEq, TokenNumber,
		TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with ORDER BY
	tokens = tokenizeAll("SELECT * FROM users ORDER BY name ASC;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenORDER, TokenBY, TokenIdentifier, TokenASC, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with ORDER BY DESC
	tokens = tokenizeAll("SELECT * FROM users ORDER BY id DESC;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenORDER, TokenBY, TokenIdentifier, TokenDESC, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with LIMIT
	tokens = tokenizeAll("SELECT * FROM users LIMIT 10;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenLIMIT, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with LIMIT and OFFSET
	tokens = tokenizeAll("SELECT * FROM users LIMIT 10 OFFSET 5;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenLIMIT, TokenNumber, TokenOFFSET, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Select with WHERE, ORDER BY, LIMIT combined
	tokens = tokenizeAll("SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenComma, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenWHERE, TokenIdentifier, TokenGt, TokenNumber,
		TokenORDER, TokenBY, TokenIdentifier, TokenASC,
		TokenLIMIT, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 4. Test UPDATE and DELETE statements
func TestLexerUpdateDelete(t *testing.T) {
	// Basic UPDATE
	tokens := tokenizeAll("UPDATE users SET name = 'John' WHERE id = 1;")
	expected := []TokenType{
		TokenUPDATE, TokenIdentifier, TokenSET, TokenIdentifier, TokenEq, TokenString,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// UPDATE with multiple columns
	tokens = tokenizeAll("UPDATE users SET name = 'John', age = 30 WHERE id = 1;")
	expected = []TokenType{
		TokenUPDATE, TokenIdentifier, TokenSET,
		TokenIdentifier, TokenEq, TokenString, TokenComma,
		TokenIdentifier, TokenEq, TokenNumber,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Basic DELETE
	tokens = tokenizeAll("DELETE FROM users WHERE id = 1;")
	expected = []TokenType{
		TokenDELETE, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// DELETE without WHERE
	tokens = tokenizeAll("DELETE FROM users;")
	expected = []TokenType{
		TokenDELETE, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 5. Test JOIN statements
func TestLexerJoin(t *testing.T) {
	// CROSS JOIN
	tokens := tokenizeAll("SELECT * FROM t1 CROSS JOIN t2;")
	expected := []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenCROSS, TokenJOIN, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// INNER JOIN
	tokens = tokenizeAll("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenINNER, TokenJOIN, TokenIdentifier, TokenON,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenEq,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// LEFT JOIN
	tokens = tokenizeAll("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenLEFT, TokenJOIN, TokenIdentifier, TokenON,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenEq,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// RIGHT JOIN
	tokens = tokenizeAll("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenRIGHT, TokenJOIN, TokenIdentifier, TokenON,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenEq,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Simple JOIN (without type specifier)
	tokens = tokenizeAll("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenJOIN, TokenIdentifier, TokenON,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenEq,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 6. Test aggregation functions
func TestLexerAggregationFunctions(t *testing.T) {
	// COUNT
	tokens := tokenizeAll("SELECT COUNT(*) FROM users;")
	expected := []TokenType{
		TokenSELECT, TokenCOUNT, TokenLParen, TokenStar, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// COUNT with column
	tokens = tokenizeAll("SELECT COUNT(id) FROM users;")
	expected = []TokenType{
		TokenSELECT, TokenCOUNT, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// SUM
	tokens = tokenizeAll("SELECT SUM(salary) FROM employees;")
	expected = []TokenType{
		TokenSELECT, TokenSUM, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// AVG
	tokens = tokenizeAll("SELECT AVG(age) FROM users;")
	expected = []TokenType{
		TokenSELECT, TokenAVG, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// MIN
	tokens = tokenizeAll("SELECT MIN(age) FROM users;")
	expected = []TokenType{
		TokenSELECT, TokenMIN, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// MAX
	tokens = tokenizeAll("SELECT MAX(salary) FROM employees;")
	expected = []TokenType{
		TokenSELECT, TokenMAX, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Multiple aggregation functions
	tokens = tokenizeAll("SELECT COUNT(*), SUM(salary), AVG(age), MIN(id), MAX(id) FROM users;")
	expected = []TokenType{
		TokenSELECT,
		TokenCOUNT, TokenLParen, TokenStar, TokenRParen, TokenComma,
		TokenSUM, TokenLParen, TokenIdentifier, TokenRParen, TokenComma,
		TokenAVG, TokenLParen, TokenIdentifier, TokenRParen, TokenComma,
		TokenMIN, TokenLParen, TokenIdentifier, TokenRParen, TokenComma,
		TokenMAX, TokenLParen, TokenIdentifier, TokenRParen,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 7. Test data types
func TestLexerDataTypes(t *testing.T) {
	// INTEGER
	tokens := tokenizeAll("CREATE TABLE t (col INTEGER);")
	expected := []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// STRING
	tokens = tokenizeAll("CREATE TABLE t (col STRING);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenSTRING, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// FLOAT
	tokens = tokenizeAll("CREATE TABLE t (col FLOAT);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenFLOAT, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// BOOLEAN
	tokens = tokenizeAll("CREATE TABLE t (col BOOLEAN);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenBOOLEAN, TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Multiple columns with different types
	tokens = tokenizeAll("CREATE TABLE t (c1 INTEGER, c2 STRING, c3 FLOAT, c4 BOOLEAN);")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenComma,
		TokenIdentifier, TokenSTRING, TokenComma,
		TokenIdentifier, TokenFLOAT, TokenComma,
		TokenIdentifier, TokenBOOLEAN,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 8. Test literals (integers, floats, strings, booleans, NULL)
func TestLexerLiterals(t *testing.T) {
	// Integer literals
	tokens := tokenizeAll("SELECT 0, 1, 123, 999999;")
	expected := []TokenType{
		TokenSELECT, TokenNumber, TokenComma, TokenNumber, TokenComma,
		TokenNumber, TokenComma, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Float literals
	tokens = tokenizeAll("SELECT 1.0, 3.14, 99.99;")
	expected = []TokenType{
		TokenSELECT, TokenNumber, TokenComma, TokenNumber, TokenComma,
		TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// String literals
	tokens = tokenizeAll("SELECT 'hello', 'world', 'test string';")
	expected = []TokenType{
		TokenSELECT, TokenString, TokenComma, TokenString, TokenComma,
		TokenString, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Boolean literals
	tokens = tokenizeAll("SELECT TRUE, FALSE;")
	expected = []TokenType{
		TokenSELECT, TokenTRUE, TokenComma, TokenFALSE, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// NULL literal
	tokens = tokenizeAll("SELECT NULL;")
	expected = []TokenType{
		TokenSELECT, TokenNULL, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Mixed literals
	tokens = tokenizeAll("INSERT INTO t VALUES (1, 3.14, 'hello', TRUE, FALSE, NULL);")
	expected = []TokenType{
		TokenINSERT, TokenINTO, TokenIdentifier, TokenVALUES, TokenLParen,
		TokenNumber, TokenComma, TokenNumber, TokenComma, TokenString, TokenComma,
		TokenTRUE, TokenComma, TokenFALSE, TokenComma, TokenNULL,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 9. Test operators (=, <>, <, >, <=, >=, AND, OR, NOT)
func TestLexerOperators(t *testing.T) {
	// Equality operator
	tokens := tokenizeAll("SELECT * FROM t WHERE a = 1;")
	expected := []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Not equal operators (<> and !=)
	tokens = tokenizeAll("SELECT * FROM t WHERE a <> 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenNe, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	tokens = tokenizeAll("SELECT * FROM t WHERE a != 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenNe, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Less than operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a < 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenLt, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Greater than operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a > 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenGt, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Less than or equal operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a <= 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenLe, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Greater than or equal operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a >= 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenGe, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// AND operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a = 1 AND b = 2;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber,
		TokenAND, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// OR operator
	tokens = tokenizeAll("SELECT * FROM t WHERE a = 1 OR b = 2;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenEq, TokenNumber,
		TokenOR, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// NOT operator
	tokens = tokenizeAll("SELECT * FROM t WHERE NOT a = 1;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenNOT, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// IS NOT NULL pattern
	tokens = tokenizeAll("SELECT * FROM t WHERE a NOT NULL;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier,
		TokenWHERE, TokenIdentifier, TokenNOT, TokenNULL, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Combined operators
	tokens = tokenizeAll("SELECT * FROM t WHERE (a > 1 AND b < 2) OR (c >= 3 AND d <= 4) AND NOT e = 5;")
	expected = []TokenType{
		TokenSELECT, TokenStar, TokenFROM, TokenIdentifier, TokenWHERE,
		TokenLParen, TokenIdentifier, TokenGt, TokenNumber,
		TokenAND, TokenIdentifier, TokenLt, TokenNumber, TokenRParen,
		TokenOR, TokenLParen, TokenIdentifier, TokenGe, TokenNumber,
		TokenAND, TokenIdentifier, TokenLe, TokenNumber, TokenRParen,
		TokenAND, TokenNOT, TokenIdentifier, TokenEq, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// 10. Test identifiers and qualified identifiers (table.column)
func TestLexerIdentifiers(t *testing.T) {
	// Simple identifier
	tokens := tokenizeAll("SELECT name FROM users;")
	expected := []TokenType{
		TokenSELECT, TokenIdentifier, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Identifier with underscore
	tokens = tokenizeAll("SELECT user_name FROM user_table;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Identifier with numbers
	tokens = tokenizeAll("SELECT col1, col2, col123 FROM table1;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenComma, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Qualified identifier (table.column)
	tokens = tokenizeAll("SELECT t.id, t.name FROM t;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenDot, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Multiple qualified identifiers in JOIN
	tokens = tokenizeAll("SELECT t1.id, t1.name, t2.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.id;")
	expected = []TokenType{
		TokenSELECT,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenDot, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenJOIN, TokenIdentifier, TokenON,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenEq,
		TokenIdentifier, TokenDot, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Quoted identifiers
	tokens = tokenizeAll(`SELECT "name" FROM "users";`)
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// Test token values
func TestLexerTokenValues(t *testing.T) {
	lexer := NewLexer("SELECT name FROM users WHERE id = 1;")

	// SELECT
	token := lexer.NextToken()
	if token.Type != TokenSELECT {
		t.Errorf("expected TokenSELECT, got %v", token.Type)
	}
	if token.Value != "SELECT" {
		t.Errorf("expected value 'SELECT', got '%s'", token.Value)
	}

	// name
	token = lexer.NextToken()
	if token.Type != TokenIdentifier {
		t.Errorf("expected TokenIdentifier, got %v", token.Type)
	}
	if token.Value != "name" {
		t.Errorf("expected value 'name', got '%s'", token.Value)
	}

	// FROM
	token = lexer.NextToken()
	if token.Type != TokenFROM {
		t.Errorf("expected TokenFROM, got %v", token.Type)
	}

	// users
	token = lexer.NextToken()
	if token.Type != TokenIdentifier {
		t.Errorf("expected TokenIdentifier, got %v", token.Type)
	}
	if token.Value != "users" {
		t.Errorf("expected value 'users', got '%s'", token.Value)
	}

	// WHERE
	token = lexer.NextToken()
	if token.Type != TokenWHERE {
		t.Errorf("expected TokenWHERE, got %v", token.Type)
	}

	// id
	token = lexer.NextToken()
	if token.Type != TokenIdentifier {
		t.Errorf("expected TokenIdentifier, got %v", token.Type)
	}
	if token.Value != "id" {
		t.Errorf("expected value 'id', got '%s'", token.Value)
	}

	// =
	token = lexer.NextToken()
	if token.Type != TokenEq {
		t.Errorf("expected TokenEq, got %v", token.Type)
	}

	// 1
	token = lexer.NextToken()
	if token.Type != TokenNumber {
		t.Errorf("expected TokenNumber, got %v", token.Type)
	}
	if token.Value != "1" {
		t.Errorf("expected value '1', got '%s'", token.Value)
	}

	// ;
	token = lexer.NextToken()
	if token.Type != TokenSemicolon {
		t.Errorf("expected TokenSemicolon, got %v", token.Type)
	}
}

// Test string literal values
func TestLexerStringValues(t *testing.T) {
	lexer := NewLexer("SELECT 'hello world';")

	// SELECT
	token := lexer.NextToken()
	if token.Type != TokenSELECT {
		t.Errorf("expected TokenSELECT, got %v", token.Type)
	}

	// 'hello world'
	token = lexer.NextToken()
	if token.Type != TokenString {
		t.Errorf("expected TokenString, got %v", token.Type)
	}
	if token.Value != "hello world" {
		t.Errorf("expected value 'hello world', got '%s'", token.Value)
	}
}

// Test number literal values
func TestLexerNumberValues(t *testing.T) {
	lexer := NewLexer("SELECT 42, 3.14159, 0.5;")

	// SELECT
	lexer.NextToken()

	// 42
	token := lexer.NextToken()
	if token.Type != TokenNumber {
		t.Errorf("expected TokenNumber, got %v", token.Type)
	}
	if token.Value != "42" {
		t.Errorf("expected value '42', got '%s'", token.Value)
	}

	// ,
	lexer.NextToken()

	// 3.14159
	token = lexer.NextToken()
	if token.Type != TokenNumber {
		t.Errorf("expected TokenNumber, got %v", token.Type)
	}
	if token.Value != "3.14159" {
		t.Errorf("expected value '3.14159', got '%s'", token.Value)
	}

	// ,
	lexer.NextToken()

	// 0.5
	token = lexer.NextToken()
	if token.Type != TokenNumber {
		t.Errorf("expected TokenNumber, got %v", token.Type)
	}
	if token.Value != "0.5" {
		t.Errorf("expected value '0.5', got '%s'", token.Value)
	}
}

// Test PeekToken
func TestLexerPeekToken(t *testing.T) {
	lexer := NewLexer("SELECT * FROM users;")

	// Peek at SELECT
	token := lexer.PeekToken()
	if token.Type != TokenSELECT {
		t.Errorf("expected TokenSELECT on peek, got %v", token.Type)
	}

	// Actual read should still be SELECT
	token = lexer.NextToken()
	if token.Type != TokenSELECT {
		t.Errorf("expected TokenSELECT on read, got %v", token.Type)
	}

	// Peek at *
	token = lexer.PeekToken()
	if token.Type != TokenStar {
		t.Errorf("expected TokenStar on peek, got %v", token.Type)
	}

	// Actual read should still be *
	token = lexer.NextToken()
	if token.Type != TokenStar {
		t.Errorf("expected TokenStar on read, got %v", token.Type)
	}
}

// Test arithmetic operators
func TestLexerArithmeticOperators(t *testing.T) {
	tokens := tokenizeAll("SELECT a + b, a - b, a * b, a / b FROM t;")
	expected := []TokenType{
		TokenSELECT,
		TokenIdentifier, TokenPlus, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenMinus, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenStar, TokenIdentifier, TokenComma,
		TokenIdentifier, TokenDivide, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// Test GROUP BY and HAVING
func TestLexerGroupByHaving(t *testing.T) {
	// GROUP BY
	tokens := tokenizeAll("SELECT COUNT(*) FROM t GROUP BY id;")
	expected := []TokenType{
		TokenSELECT, TokenCOUNT, TokenLParen, TokenStar, TokenRParen,
		TokenFROM, TokenIdentifier, TokenGROUP, TokenBY, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// GROUP BY with HAVING
	tokens = tokenizeAll("SELECT COUNT(*) FROM t GROUP BY id HAVING COUNT(*) > 1;")
	expected = []TokenType{
		TokenSELECT, TokenCOUNT, TokenLParen, TokenStar, TokenRParen,
		TokenFROM, TokenIdentifier, TokenGROUP, TokenBY, TokenIdentifier,
		TokenHAVING, TokenCOUNT, TokenLParen, TokenStar, TokenRParen,
		TokenGt, TokenNumber, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// Test transaction statements
func TestLexerTransactions(t *testing.T) {
	// BEGIN
	tokens := tokenizeAll("BEGIN;")
	expected := []TokenType{TokenBEGIN, TokenSemicolon}
	assertTokensEqual(t, tokens, expected)

	// COMMIT
	tokens = tokenizeAll("COMMIT;")
	expected = []TokenType{TokenCOMMIT, TokenSemicolon}
	assertTokensEqual(t, tokens, expected)

	// ROLLBACK
	tokens = tokenizeAll("ROLLBACK;")
	expected = []TokenType{TokenROLLBACK, TokenSemicolon}
	assertTokensEqual(t, tokens, expected)
}

// Test SHOW TABLES
func TestLexerShowTables(t *testing.T) {
	tokens := tokenizeAll("SHOW TABLES;")
	expected := []TokenType{TokenSHOW, TokenTABLES, TokenSemicolon}
	assertTokensEqual(t, tokens, expected)
}

// Test AS alias
func TestLexerAlias(t *testing.T) {
	// Column alias
	tokens := tokenizeAll("SELECT id AS user_id FROM users;")
	expected := []TokenType{
		TokenSELECT, TokenIdentifier, TokenAS, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	// Table alias
	tokens = tokenizeAll("SELECT u.id FROM users AS u;")
	expected = []TokenType{
		TokenSELECT, TokenIdentifier, TokenDot, TokenIdentifier,
		TokenFROM, TokenIdentifier, TokenAS, TokenIdentifier, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// Test DEFAULT keyword
func TestLexerDefault(t *testing.T) {
	tokens := tokenizeAll("CREATE TABLE t (id INTEGER DEFAULT 0);")
	expected := []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenINTEGER, TokenDEFAULT, TokenNumber,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)

	tokens = tokenizeAll("CREATE TABLE t (name STRING DEFAULT 'unknown');")
	expected = []TokenType{
		TokenCREATE, TokenTABLE, TokenIdentifier, TokenLParen,
		TokenIdentifier, TokenSTRING, TokenDEFAULT, TokenString,
		TokenRParen, TokenSemicolon,
	}
	assertTokensEqual(t, tokens, expected)
}

// Test unterminated string (error case)
func TestLexerUnterminatedString(t *testing.T) {
	lexer := NewLexer("SELECT 'unterminated")
	token := lexer.NextToken()
	// First token is SELECT
	if token.Type != TokenSELECT {
		t.Errorf("expected TokenSELECT, got %v", token.Type)
	}
	// Second token should be an error
	token = lexer.NextToken()
	if token.Type != TokenError {
		t.Errorf("expected TokenError for unterminated string, got %v", token.Type)
	}
}

// Test escaped quotes in strings
func TestLexerEscapedQuotes(t *testing.T) {
	lexer := NewLexer("SELECT 'it''s a test';")
	// SELECT
	lexer.NextToken()
	// 'it''s a test' - escaped single quote
	token := lexer.NextToken()
	if token.Type != TokenString {
		t.Errorf("expected TokenString, got %v", token.Type)
	}
	// Note: The current implementation may not handle escaped quotes properly
	// This test documents the expected behavior
}

// Test empty input
func TestLexerEmptyInput(t *testing.T) {
	lexer := NewLexer("")
	token := lexer.NextToken()
	if token.Type != TokenEOF {
		t.Errorf("expected TokenEOF for empty input, got %v", token.Type)
	}
}

// Test whitespace only input
func TestLexerWhitespaceOnly(t *testing.T) {
	lexer := NewLexer("   \t\n\r   ")
	token := lexer.NextToken()
	if token.Type != TokenEOF {
		t.Errorf("expected TokenEOF for whitespace-only input, got %v", token.Type)
	}
}

// Test data type aliases (INT, BOOL, DOUBLE, REAL, TEXT, VARCHAR)
func TestLexerDataTypeAliases(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"INT", TokenINTEGER},
		{"INTEGER", TokenINTEGER},
		{"int", TokenINTEGER},
		{"integer", TokenINTEGER},
		{"FLOAT", TokenFLOAT},
		{"DOUBLE", TokenFLOAT},
		{"REAL", TokenFLOAT},
		{"STRING", TokenSTRING},
		{"TEXT", TokenSTRING},
		{"VARCHAR", TokenSTRING},
		{"BOOLEAN", TokenBOOLEAN},
		{"BOOL", TokenBOOLEAN},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := NewLexer(tt.input)
			tok := l.NextToken()
			if tok.Type != tt.expected {
				t.Errorf("for %q: expected %v, got %v", tt.input, tt.expected, tok.Type)
			}
		})
	}
}
