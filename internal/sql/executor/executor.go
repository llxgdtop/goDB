package executor

import (
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// Result represents an execution result
type Result struct {
	Columns []string
	Rows    []types.Row
	Message string
}

// Executor executes plan nodes
type Executor struct {
	engine SQLEngine
}

// SQLEngine is the interface for SQL engine operations
type SQLEngine interface {
	CreateTable(schema *types.Table) error
	GetTable(name string) (*types.Table, bool)
	GetTables() map[string]*types.Table
	Insert(table string, columns []string, values []types.Value) error
	// CheckDuplicate checks if a row with the given primary key already exists
	// Returns nil if no duplicate, error if duplicate exists
	CheckDuplicate(table string, values []types.Value) error
	Scan(table string, filter func(types.Row) bool) ([]types.Row, error)
	Update(table string, filter func(types.Row) bool, update func(types.Row) types.Row) (int64, error)
	Delete(table string, filter func(types.Row) bool) (int64, error)
	Begin() error
	Commit() error
	Rollback() error
	GetTransactionVersion() uint64
	IsInTransaction() bool
}

// NewExecutor creates a new executor
func NewExecutor(engine SQLEngine) *Executor {
	return &Executor{engine: engine}
}

// NewSessionExecutor creates a new executor with a session
// This is an alias for NewExecutor since Session implements SQLEngine
func NewSessionExecutor(engine SQLEngine) *Executor {
	return &Executor{engine: engine}
}

// Execute executes a plan node
func (e *Executor) Execute(node plan.Node) (*Result, error) {
	switch n := node.(type) {
	case *plan.CreateTableNode:
		return e.executeCreateTable(n)
	case *plan.InsertNode:
		return e.executeInsert(n)
	case *plan.ScanNode:
		return e.executeScan(n)
	case *plan.ProjectionNode:
		return e.executeProjection(n)
	case *plan.OrderNode:
		return e.executeOrder(n)
	case *plan.LimitNode:
		return e.executeLimit(n)
	case *plan.OffsetNode:
		return e.executeOffset(n)
	case *plan.FilterNode:
		return e.executeFilter(n)
	case *plan.NestedLoopJoinNode:
		return e.executeJoin(n)
	case *plan.AggregateNode:
		return e.executeAggregate(n)
	case *plan.UpdateNode:
		return e.executeUpdate(n)
	case *plan.DeleteNode:
		return e.executeDelete(n)
	case *plan.BeginNode:
		return e.executeBegin(n)
	case *plan.CommitNode:
		return e.executeCommit(n)
	case *plan.RollbackNode:
		return e.executeRollback(n)
	case *plan.ShowTablesNode:
		return e.executeShowTables(n)
	case *plan.ShowTableNode:
		return e.executeShowTable(n)
	default:
		return nil, nil
	}
}
