package plan

import (
	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// Planner creates execution plans from AST statements
type Planner struct {
	tables map[string]*types.Table
}

// NewPlanner creates a new planner
func NewPlanner(tables map[string]*types.Table) *Planner {
	return &Planner{tables: tables}
}

// Plan creates an execution plan from a statement
func (p *Planner) Plan(stmt parser.Statement) (Node, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStatement:
		return p.planCreateTable(s)
	case *parser.InsertStatement:
		return p.planInsert(s)
	case *parser.SelectStatement:
		return p.planSelect(s)
	case *parser.UpdateStatement:
		return p.planUpdate(s)
	case *parser.DeleteStatement:
		return p.planDelete(s)
	case *parser.BeginStatement:
		return &BeginNode{}, nil
	case *parser.CommitStatement:
		return &CommitNode{}, nil
	case *parser.RollbackStatement:
		return &RollbackNode{}, nil
	case *parser.ShowTablesStatement:
		return &ShowTablesNode{}, nil
	case *parser.ShowTableStatement:
		return &ShowTableNode{Name: s.Name}, nil
	default:
		return nil, dberror.NewInternalError("unsupported statement type")
	}
}

func (p *Planner) planCreateTable(s *parser.CreateTableStatement) (Node, error) {
	// Check if table already exists
	if _, exists := p.tables[s.Name]; exists {
		return nil, dberror.NewTableExistsError(s.Name)
	}

	// Convert column definitions
	var columns []types.Column
	for _, colDef := range s.Columns {
		var defaultVal *types.Value
		if colDef.Default != nil {
			val, err := p.evaluateLiteral(*colDef.Default)
			if err != nil {
				return nil, err
			}
			defaultVal = &val
		}

		columns = append(columns, types.Column{
			Name:       colDef.Name,
			DataType:   colDef.DataType,
			Nullable:   colDef.Nullable,
			Default:    defaultVal,
			PrimaryKey: colDef.PrimaryKey,
		})
	}

	return &CreateTableNode{
		Schema: &types.Table{
			Name:    s.Name,
			Columns: columns,
		},
	}, nil
}

func (p *Planner) planInsert(s *parser.InsertStatement) (Node, error) {
	return &InsertNode{
		TableName: s.TableName,
		Columns:   s.Columns,
		Values:    s.Values,
	}, nil
}

func (p *Planner) planSelect(s *parser.SelectStatement) (Node, error) {
	// Plan FROM clause
	var source Node
	if s.From != nil {
		var err error
		source, err = p.planFromItem(s.From)
		if err != nil {
			return nil, err
		}
	}

	// Plan WHERE clause - wrap source in FilterNode
	if s.Where != nil && source != nil {
		source = &FilterNode{
			Source:    source,
			Predicate: *s.Where,
		}
	}

	// Plan GROUP BY and aggregation
	if len(s.GroupBy) > 0 || p.hasAggregates(s.Select) {
		source = &AggregateNode{
			Source:  source,
			Exprs:   convertSelectExprs(s.Select),
			GroupBy: s.GroupBy,
		}

		// Plan HAVING
		if s.Having != nil {
			source = &FilterNode{
				Source:    source,
				Predicate: *s.Having,
			}
		}
	}

	// Plan projection
	if !p.isSelectStar(s.Select) || len(s.Select) > 0 {
		source = &ProjectionNode{
			Source: source,
			Exprs:  convertSelectExprs(s.Select),
		}
	}

	// Plan ORDER BY
	if len(s.OrderBy) > 0 {
		orderByItems := make([]OrderByItem, len(s.OrderBy))
		for i, item := range s.OrderBy {
			orderByItems[i] = OrderByItem{
				Expr: item.Expr,
				Desc: item.Desc,
			}
		}
		source = &OrderNode{
			Source:  source,
			OrderBy: orderByItems,
		}
	}

	// Plan LIMIT
	if s.Limit != nil {
		limit, err := p.evaluateLiteral(*s.Limit)
		if err != nil {
			return nil, err
		}
		source = &LimitNode{
			Source: source,
			Limit:  limit.Int,
		}
	}

	// Plan OFFSET
	if s.Offset != nil {
		offset, err := p.evaluateLiteral(*s.Offset)
		if err != nil {
			return nil, err
		}
		source = &OffsetNode{
			Source: source,
			Offset: offset.Int,
		}
	}

	return source, nil
}

func (p *Planner) planFromItem(item parser.FromItem) (Node, error) {
	switch fi := item.(type) {
	case *parser.TableFromItem:
		return &ScanNode{TableName: fi.Name}, nil
	case *parser.JoinFromItem:
		left, err := p.planFromItem(fi.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.planFromItem(fi.Right)
		if err != nil {
			return nil, err
		}
		return &NestedLoopJoinNode{
			Left:      left,
			Right:     right,
			Predicate: fi.Predicate,
			JoinType:  fi.JoinType,
		}, nil
	default:
		return nil, dberror.NewInternalError("unsupported from item type")
	}
}

func (p *Planner) planUpdate(s *parser.UpdateStatement) (Node, error) {
	source := Node(&ScanNode{TableName: s.TableName})

	if s.Where != nil {
		source = &FilterNode{
			Source:    source,
			Predicate: *s.Where,
		}
	}

	return &UpdateNode{
		TableName: s.TableName,
		Source:    source,
		Columns:   s.Columns,
	}, nil
}

func (p *Planner) planDelete(s *parser.DeleteStatement) (Node, error) {
	source := Node(&ScanNode{TableName: s.TableName})

	if s.Where != nil {
		source = &FilterNode{
			Source:    source,
			Predicate: *s.Where,
		}
	}

	return &DeleteNode{
		TableName: s.TableName,
		Source:    source,
	}, nil
}

func (p *Planner) evaluateLiteral(expr parser.Expression) (types.Value, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpression:
		return e.Value, nil
	default:
		return types.Value{}, dberror.NewInternalError("expected literal expression")
	}
}

func (p *Planner) hasAggregates(selectItems []parser.SelectItem) bool {
	for _, item := range selectItems {
		if p.exprHasAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func (p *Planner) exprHasAggregate(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.FunctionCall:
		return true
	case *parser.BinaryOperation:
		return p.exprHasAggregate(e.Left) || p.exprHasAggregate(e.Right)
	case *parser.UnaryOperation:
		return p.exprHasAggregate(e.Expr)
	default:
		return false
	}
}

func (p *Planner) isSelectStar(selectItems []parser.SelectItem) bool {
	if len(selectItems) == 1 {
		if _, ok := selectItems[0].Expr.(*parser.StarExpression); ok {
			return true
		}
	}
	return false
}

func convertSelectExprs(selectItems []parser.SelectItem) []SelectExpr {
	exprs := make([]SelectExpr, len(selectItems))
	for i, item := range selectItems {
		exprs[i] = SelectExpr{
			Expr: item.Expr,
			As:   item.As,
		}
	}
	return exprs
}
