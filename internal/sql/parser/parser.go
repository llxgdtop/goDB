package parser

import (
	"strconv"
	"strings"

	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// Parser parses SQL statements
type Parser struct {
	lexer *Lexer
	token Token
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	p := &Parser{lexer: lexer}
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.token = p.lexer.NextToken()
}

func (p *Parser) expect(t TokenType) error {
	if p.token.Type != t {
		return dberror.NewParseError("expected %v, got %v (%s)", t, p.token.Type, p.token.Value)
	}
	p.nextToken()
	return nil
}

func (p *Parser) expectIdentifier() (string, error) {
	if p.token.Type != TokenIdentifier {
		return "", dberror.NewParseError("expected identifier, got %v", p.token.Type)
	}
	name := p.token.Value
	p.nextToken()
	return name, nil
}

// Parse parses a SQL statement
func (p *Parser) Parse() (Statement, error) {
	if p.token.Type == TokenEOF {
		return nil, nil
	}

	switch p.token.Type {
	case TokenCREATE:
		return p.parseCreate()
	case TokenINSERT:
		return p.parseInsert()
	case TokenSELECT:
		return p.parseSelect()
	case TokenUPDATE:
		return p.parseUpdate()
	case TokenDELETE:
		return p.parseDelete()
	case TokenBEGIN:
		p.nextToken()
		return &BeginStatement{}, nil
	case TokenCOMMIT:
		p.nextToken()
		return &CommitStatement{}, nil
	case TokenROLLBACK:
		p.nextToken()
		return &RollbackStatement{}, nil
	case TokenSHOW:
		return p.parseShow()
	default:
		return nil, dberror.NewParseError("unexpected token: %v", p.token.Type)
	}
}

func (p *Parser) parseCreate() (Statement, error) {
	if err := p.expect(TokenCREATE); err != nil {
		return nil, err
	}
	if err := p.expect(TokenTABLE); err != nil {
		return nil, err
	}

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	if err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)

		if p.token.Type == TokenRParen {
			p.nextToken()
			break
		}
		if err := p.expect(TokenComma); err != nil {
			return nil, err
		}
	}

	return &CreateTableStatement{Name: name, Columns: columns}, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expectIdentifier()
	if err != nil {
		return ColumnDef{}, err
	}

	dataType, err := p.parseDataType()
	if err != nil {
		return ColumnDef{}, err
	}

	col := ColumnDef{
		Name:     name,
		DataType: dataType,
		Nullable: true,
	}

	// Parse constraints
	for {
		switch p.token.Type {
		case TokenNOT:
			p.nextToken()
			if err := p.expect(TokenNULL); err != nil {
				return ColumnDef{}, err
			}
			col.Nullable = false
		case TokenPRIMARY:
			p.nextToken()
			if err := p.expect(TokenKEY); err != nil {
				return ColumnDef{}, err
			}
			col.PrimaryKey = true
			col.Nullable = false
		case TokenDEFAULT:
			p.nextToken()
			expr, err := p.parseExpression()
			if err != nil {
				return ColumnDef{}, err
			}
			col.Default = &expr
		default:
			return col, nil
		}
	}
}

func (p *Parser) parseDataType() (types.DataType, error) {
	switch p.token.Type {
	case TokenBOOLEAN:
		p.nextToken()
		return types.TypeBoolean, nil
	case TokenINTEGER:
		p.nextToken()
		return types.TypeInteger, nil
	case TokenFLOAT:
		p.nextToken()
		return types.TypeFloat, nil
	case TokenSTRING:
		p.nextToken()
		return types.TypeString, nil
	default:
		return 0, dberror.NewParseError("expected data type, got %v", p.token.Type)
	}
}

func (p *Parser) parseInsert() (Statement, error) {
	if err := p.expect(TokenINSERT); err != nil {
		return nil, err
	}
	if err := p.expect(TokenINTO); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	var columns []string
	if p.token.Type == TokenLParen {
		p.nextToken()
		for {
			col, err := p.expectIdentifier()
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)

			if p.token.Type == TokenRParen {
				p.nextToken()
				break
			}
			if err := p.expect(TokenComma); err != nil {
				return nil, err
			}
		}
	}

	if err := p.expect(TokenVALUES); err != nil {
		return nil, err
	}

	var values [][]Expression
	for {
		if err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		var row []Expression
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			row = append(row, expr)

			if p.token.Type == TokenRParen {
				p.nextToken()
				break
			}
			if err := p.expect(TokenComma); err != nil {
				return nil, err
			}
		}
		values = append(values, row)

		if p.token.Type != TokenComma {
			break
		}
		p.nextToken()
	}

	return &InsertStatement{TableName: tableName, Columns: columns, Values: values}, nil
}

func (p *Parser) parseSelect() (Statement, error) {
	if err := p.expect(TokenSELECT); err != nil {
		return nil, err
	}

	// Parse select items
	var selectItems []SelectItem
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		selectItems = append(selectItems, item)

		if p.token.Type != TokenComma {
			break
		}
		p.nextToken()
	}

	// Parse FROM
	var from FromItem
	if p.token.Type == TokenFROM {
		p.nextToken()
		var err error
		from, err = p.parseFromItem()
		if err != nil {
			return nil, err
		}
	} else {
		from = &TableFromItem{}
	}

	// Parse WHERE
	var where *Expression
	if p.token.Type == TokenWHERE {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		where = &expr
	}

	// Parse GROUP BY
	var groupBy []Expression
	if p.token.Type == TokenGROUP {
		p.nextToken()
		if err := p.expect(TokenBY); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			groupBy = append(groupBy, expr)

			if p.token.Type != TokenComma {
				break
			}
			p.nextToken()
		}
	}

	// Parse HAVING
	var having *Expression
	if p.token.Type == TokenHAVING {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		having = &expr
	}

	// Parse ORDER BY
	var orderBy []OrderByItem
	if p.token.Type == TokenORDER {
		p.nextToken()
		if err := p.expect(TokenBY); err != nil {
			return nil, err
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			item := OrderByItem{Expr: expr}
			if p.token.Type == TokenDESC {
				item.Desc = true
				p.nextToken()
			} else if p.token.Type == TokenASC {
				p.nextToken()
			}
			orderBy = append(orderBy, item)

			if p.token.Type != TokenComma {
				break
			}
			p.nextToken()
		}
	}

	// Parse LIMIT
	var limit *Expression
	if p.token.Type == TokenLIMIT {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		limit = &expr
	}

	// Parse OFFSET
	var offset *Expression
	if p.token.Type == TokenOFFSET {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		offset = &expr
	}

	return &SelectStatement{
		Select:  selectItems,
		From:    from,
		Where:   where,
		GroupBy: groupBy,
		Having:  having,
		OrderBy: orderBy,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

func (p *Parser) parseSelectItem() (SelectItem, error) {
	// Check for *
	if p.token.Type == TokenStar {
		p.nextToken()
		return SelectItem{Expr: &StarExpression{}}, nil
	}

	// Check for aggregate functions
	if p.token.Type == TokenCOUNT || p.token.Type == TokenMIN || p.token.Type == TokenMAX ||
		p.token.Type == TokenSUM || p.token.Type == TokenAVG {
		funcName := strings.ToUpper(p.token.Value) // Normalize to uppercase for consistent comparison
		p.nextToken()
		if err := p.expect(TokenLParen); err != nil {
			return SelectItem{}, err
		}

		var arg Expression
		if p.token.Type == TokenStar {
			p.nextToken()
			arg = &StarExpression{}
		} else {
			var err error
			arg, err = p.parseExpression()
			if err != nil {
				return SelectItem{}, err
			}
		}

		if err := p.expect(TokenRParen); err != nil {
			return SelectItem{}, err
		}

		item := SelectItem{Expr: &FunctionCall{Name: funcName, Arg: arg}}

		// Check for AS
		if p.token.Type == TokenAS {
			p.nextToken()
			as, err := p.expectIdentifier()
			if err != nil {
				return SelectItem{}, err
			}
			item.As = as
		}

		return item, nil
	}

	expr, err := p.parseExpression()
	if err != nil {
		return SelectItem{}, err
	}

	item := SelectItem{Expr: expr}

	// Check for AS
	if p.token.Type == TokenAS {
		p.nextToken()
		as, err := p.expectIdentifier()
		if err != nil {
			return SelectItem{}, err
		}
		item.As = as
	}

	return item, nil
}

func (p *Parser) parseFromItem() (FromItem, error) {
	left, err := p.parseTableFromItem()
	if err != nil {
		return nil, err
	}

	for {
		var joinType JoinType
		switch p.token.Type {
		case TokenCROSS:
			p.nextToken()
			if err := p.expect(TokenJOIN); err != nil {
				return nil, err
			}
			joinType = JoinCross
		case TokenINNER:
			p.nextToken()
			if err := p.expect(TokenJOIN); err != nil {
				return nil, err
			}
			joinType = JoinInner
		case TokenLEFT:
			p.nextToken()
			if err := p.expect(TokenJOIN); err != nil {
				return nil, err
			}
			joinType = JoinLeft
		case TokenRIGHT:
			p.nextToken()
			if err := p.expect(TokenJOIN); err != nil {
				return nil, err
			}
			joinType = JoinRight
		case TokenJOIN:
			p.nextToken()
			joinType = JoinInner
		default:
			return left, nil
		}

		right, err := p.parseTableFromItem()
		if err != nil {
			return nil, err
		}

		var predicate *Expression
		if p.token.Type == TokenON {
			p.nextToken()
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			predicate = &expr
		}

		left = &JoinFromItem{
			Left:      left,
			Right:     right,
			JoinType:  joinType,
			Predicate: predicate,
		}
	}
}

func (p *Parser) parseTableFromItem() (FromItem, error) {
	name, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	as := ""
	if p.token.Type == TokenAS {
		p.nextToken()
		as, err = p.expectIdentifier()
		if err != nil {
			return nil, err
		}
	}

	return &TableFromItem{Name: name, As: as}, nil
}

func (p *Parser) parseUpdate() (Statement, error) {
	if err := p.expect(TokenUPDATE); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	if err := p.expect(TokenSET); err != nil {
		return nil, err
	}

	columns := make(map[string]Expression)
	for {
		col, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		columns[col] = expr

		if p.token.Type != TokenComma {
			break
		}
		p.nextToken()
	}

	var where *Expression
	if p.token.Type == TokenWHERE {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		where = &expr
	}

	return &UpdateStatement{TableName: tableName, Columns: columns, Where: where}, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	if err := p.expect(TokenDELETE); err != nil {
		return nil, err
	}
	if err := p.expect(TokenFROM); err != nil {
		return nil, err
	}

	tableName, err := p.expectIdentifier()
	if err != nil {
		return nil, err
	}

	var where *Expression
	if p.token.Type == TokenWHERE {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		where = &expr
	}

	return &DeleteStatement{TableName: tableName, Where: where}, nil
}

func (p *Parser) parseShow() (Statement, error) {
	if err := p.expect(TokenSHOW); err != nil {
		return nil, err
	}

	if p.token.Type == TokenTABLES {
		p.nextToken()
		return &ShowTablesStatement{}, nil
	}

	if p.token.Type == TokenTABLE {
		p.nextToken()
		name, err := p.expectIdentifier()
		if err != nil {
			return nil, err
		}
		return &ShowTableStatement{Name: name}, nil
	}

	return nil, dberror.NewParseError("expected TABLES or TABLE after SHOW")
}

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.token.Type == TokenOR {
		p.nextToken()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryOperation{Left: left, Op: OpOr, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAnd() (Expression, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.token.Type == TokenAND {
		p.nextToken()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryOperation{Left: left, Op: OpAnd, Right: right}
	}

	return left, nil
}

func (p *Parser) parseComparison() (Expression, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	var op BinaryOp
	switch p.token.Type {
	case TokenEq:
		op = OpEq
	case TokenNe:
		op = OpNe
	case TokenLt:
		op = OpLt
	case TokenLe:
		op = OpLe
	case TokenGt:
		op = OpGt
	case TokenGe:
		op = OpGe
	default:
		return left, nil
	}

	p.nextToken()
	right, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	return &BinaryOperation{Left: left, Op: op, Right: right}, nil
}

func (p *Parser) parseAdditive() (Expression, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for {
		var op BinaryOp
		switch p.token.Type {
		case TokenPlus:
			op = OpAdd
		case TokenMinus:
			op = OpSub
		default:
			return left, nil
		}

		p.nextToken()
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &BinaryOperation{Left: left, Op: op, Right: right}
	}
}

func (p *Parser) parseMultiplicative() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		var op BinaryOp
		switch p.token.Type {
		case TokenStar:
			op = OpMul
		case TokenDivide:
			op = OpDiv
		default:
			return left, nil
		}

		p.nextToken()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryOperation{Left: left, Op: op, Right: right}
	}
}

func (p *Parser) parseUnary() (Expression, error) {
	switch p.token.Type {
	case TokenNOT:
		p.nextToken()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOperation{Op: OpNot, Expr: expr}, nil
	case TokenMinus:
		p.nextToken()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOperation{Op: OpNeg, Expr: expr}, nil
	default:
		return p.parsePrimary()
	}
}

func (p *Parser) parsePrimary() (Expression, error) {
	switch p.token.Type {
	case TokenNULL:
		p.nextToken()
		return &LiteralExpression{Value: types.NewNullValue()}, nil
	case TokenTRUE:
		p.nextToken()
		return &LiteralExpression{Value: types.NewBoolValue(true)}, nil
	case TokenFALSE:
		p.nextToken()
		return &LiteralExpression{Value: types.NewBoolValue(false)}, nil
	case TokenString:
		value := p.token.Value
		p.nextToken()
		return &LiteralExpression{Value: types.NewStringValue(value)}, nil
	case TokenNumber:
		value := p.token.Value
		p.nextToken()
		// Try to parse as integer first
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return &LiteralExpression{Value: types.NewIntValue(i)}, nil
		}
		// Parse as float
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, dberror.NewParseError("invalid number: %s", value)
		}
		return &LiteralExpression{Value: types.NewFloatValue(f)}, nil
	case TokenIdentifier:
		name := p.token.Value
		p.nextToken()

		// Check for qualified identifier (table.column)
		if p.token.Type == TokenDot {
			p.nextToken()
			col, err := p.expectIdentifier()
			if err != nil {
				return nil, err
			}
			return &QualifiedIdentifierExpression{Table: name, Column: col}, nil
		}

		return &IdentifierExpression{Name: name}, nil
	case TokenLParen:
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return expr, nil
	case TokenCOUNT, TokenMIN, TokenMAX, TokenSUM, TokenAVG:
		funcName := strings.ToUpper(p.token.Value) // Normalize to uppercase for consistent comparison
		p.nextToken()
		if err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		var arg Expression
		if p.token.Type == TokenStar {
			p.nextToken()
			arg = &StarExpression{}
		} else {
			var err error
			arg, err = p.parseExpression()
			if err != nil {
				return nil, err
			}
		}

		if err := p.expect(TokenRParen); err != nil {
			return nil, err
		}

		return &FunctionCall{Name: funcName, Arg: arg}, nil
	default:
		return nil, dberror.NewParseError("unexpected token: %v (%s)", p.token.Type, p.token.Value)
	}
}

// ParseStatement parses a single SQL statement
func ParseStatement(input string) (Statement, error) {
	parser := NewParser(input)
	return parser.Parse()
}

// ParseStatements parses multiple SQL statements separated by semicolons
func ParseStatements(input string) ([]Statement, error) {
	parser := NewParser(input)
	var statements []Statement

	for {
		stmt, err := parser.Parse()
		if err != nil {
			return nil, err
		}
		if stmt == nil {
			break
		}
		statements = append(statements, stmt)

		// Skip optional semicolon
		if parser.token.Type == TokenSemicolon {
			parser.nextToken()
		}

		if parser.token.Type == TokenEOF {
			break
		}
	}

	return statements, nil
}
