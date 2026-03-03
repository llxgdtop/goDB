package parser

import (
	"github.com/llxgdtop/godb/internal/sql/types"
)

// Statement represents a SQL statement
type Statement interface {
	stmtNode()
}

// CreateTableStatement represents CREATE TABLE statement
type CreateTableStatement struct {
	Name    string
	Columns []ColumnDef
}

func (s *CreateTableStatement) stmtNode() {}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name       string
	DataType   types.DataType
	Nullable   bool
	Default    *Expression
	PrimaryKey bool
}

// InsertStatement represents INSERT statement
type InsertStatement struct {
	TableName string
	Columns   []string
	Values    [][]Expression
}

func (s *InsertStatement) stmtNode() {}

// SelectStatement represents SELECT statement
type SelectStatement struct {
	Select      []SelectItem
	From        FromItem
	Where       *Expression
	GroupBy     []Expression
	Having      *Expression
	OrderBy     []OrderByItem
	Limit       *Expression
	Offset      *Expression
	Distinct    bool
}

func (s *SelectStatement) stmtNode() {}

// SelectItem represents a SELECT item
type SelectItem struct {
	Expr Expression
	As   string
}

// OrderByItem represents an ORDER BY item
type OrderByItem struct {
	Expr Expression
	Desc bool
}

// UpdateStatement represents UPDATE statement
type UpdateStatement struct {
	TableName string
	Columns   map[string]Expression
	Where     *Expression
}

func (s *UpdateStatement) stmtNode() {}

// DeleteStatement represents DELETE statement
type DeleteStatement struct {
	TableName string
	Where     *Expression
}

func (s *DeleteStatement) stmtNode() {}

// BeginStatement represents BEGIN statement
type BeginStatement struct{}

func (s *BeginStatement) stmtNode() {}

// CommitStatement represents COMMIT statement
type CommitStatement struct{}

func (s *CommitStatement) stmtNode() {}

// RollbackStatement represents ROLLBACK statement
type RollbackStatement struct{}

func (s *RollbackStatement) stmtNode() {}

// ShowTablesStatement represents SHOW TABLES statement
type ShowTablesStatement struct{}

func (s *ShowTablesStatement) stmtNode() {}

// ShowTableStatement represents SHOW TABLE <name> statement
type ShowTableStatement struct {
	Name string
}

func (s *ShowTableStatement) stmtNode() {}

// FromItem represents a FROM clause item
type FromItem interface {
	fromItemNode()
}

// TableFromItem represents a table in FROM clause
type TableFromItem struct {
	Name string
	As   string
}

func (f *TableFromItem) fromItemNode() {}

// JoinFromItem represents a JOIN in FROM clause
type JoinFromItem struct {
	Left      FromItem
	Right     FromItem
	JoinType  JoinType
	Predicate *Expression
}

func (f *JoinFromItem) fromItemNode() {}

// JoinType represents the type of join
type JoinType int

const (
	JoinCross JoinType = iota
	JoinInner
	JoinLeft
	JoinRight
)

// Expression represents an expression
type Expression interface {
	exprNode()
}

// LiteralExpression represents a literal value
type LiteralExpression struct {
	Value types.Value
}

func (e *LiteralExpression) exprNode() {}

// IdentifierExpression represents an identifier
type IdentifierExpression struct {
	Name string
}

func (e *IdentifierExpression) exprNode() {}

// QualifiedIdentifierExpression represents a qualified identifier (table.column)
type QualifiedIdentifierExpression struct {
	Table  string
	Column string
}

func (e *QualifiedIdentifierExpression) exprNode() {}

// BinaryOperation represents a binary operation
type BinaryOperation struct {
	Left  Expression
	Op    BinaryOp
	Right Expression
}

func (e *BinaryOperation) exprNode() {}

// BinaryOp represents a binary operator
type BinaryOp int

const (
	OpEq BinaryOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpLike
)

// UnaryOperation represents a unary operation
type UnaryOperation struct {
	Op   UnaryOp
	Expr Expression
}

func (e *UnaryOperation) exprNode() {}

// UnaryOp represents a unary operator
type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
)

// FunctionCall represents a function call (aggregates)
type FunctionCall struct {
	Name string
	Arg  Expression
}

func (e *FunctionCall) exprNode() {}

// StarExpression represents SELECT *
type StarExpression struct{}

func (e *StarExpression) exprNode() {}

// IsNullExpression represents IS NULL
type IsNullExpression struct {
	Expr Expression
	Not  bool
}

func (e *IsNullExpression) exprNode() {}
