package plan

import (
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// Node represents an execution plan node
type Node interface {
	node()
}

// CreateTableNode represents CREATE TABLE execution
type CreateTableNode struct {
	Schema *types.Table
}

func (n *CreateTableNode) node() {}

// InsertNode represents INSERT execution
type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]parser.Expression
}

func (n *InsertNode) node() {}

// ScanNode represents table scan
type ScanNode struct {
	TableName string
	Filter    *parser.Expression
}

func (n *ScanNode) node() {}

// UpdateNode represents UPDATE execution
type UpdateNode struct {
	TableName string
	Source    Node
	Columns   map[string]parser.Expression
}

func (n *UpdateNode) node() {}

// DeleteNode represents DELETE execution
type DeleteNode struct {
	TableName string
	Source    Node
}

func (n *DeleteNode) node() {}

// OrderNode represents ORDER BY
type OrderNode struct {
	Source  Node
	OrderBy []OrderByItem
}

func (n *OrderNode) node() {}

// OrderByItem represents an order by item
type OrderByItem struct {
	Expr parser.Expression
	Desc bool
}

// LimitNode represents LIMIT
type LimitNode struct {
	Source Node
	Limit  int64
}

func (n *LimitNode) node() {}

// OffsetNode represents OFFSET
type OffsetNode struct {
	Source Node
	Offset int64
}

func (n *OffsetNode) node() {}

// ProjectionNode represents column projection
type ProjectionNode struct {
	Source Node
	Exprs  []SelectExpr
}

func (n *ProjectionNode) node() {}

// SelectExpr represents a select expression
type SelectExpr struct {
	Expr parser.Expression
	As   string
}

// NestedLoopJoinNode represents nested loop join
type NestedLoopJoinNode struct {
	Left      Node
	Right     Node
	Predicate *parser.Expression
	JoinType  parser.JoinType
}

func (n *NestedLoopJoinNode) node() {}

// AggregateNode represents aggregation
type AggregateNode struct {
	Source   Node
	Exprs    []SelectExpr
	GroupBy  []parser.Expression
}

func (n *AggregateNode) node() {}

// FilterNode represents filtering (for HAVING)
type FilterNode struct {
	Source    Node
	Predicate parser.Expression
}

func (n *FilterNode) node() {}

// BeginNode represents BEGIN
type BeginNode struct{}

func (n *BeginNode) node() {}

// CommitNode represents COMMIT
type CommitNode struct{}

func (n *CommitNode) node() {}

// RollbackNode represents ROLLBACK
type RollbackNode struct{}

func (n *RollbackNode) node() {}

// ShowTablesNode represents SHOW TABLES
type ShowTablesNode struct{}

func (n *ShowTablesNode) node() {}

// ShowTableNode represents SHOW TABLE <name>
type ShowTableNode struct {
	Name string
}

func (n *ShowTableNode) node() {}
