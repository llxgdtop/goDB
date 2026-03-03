package executor

import (
	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

func (e *Executor) executeJoin(node *plan.NestedLoopJoinNode) (*Result, error) {
	leftResult, err := e.Execute(node.Left)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.Execute(node.Right)
	if err != nil {
		return nil, err
	}

	// Get schemas
	var leftSchema, rightSchema *types.Table
	if scan, ok := node.Left.(*plan.ScanNode); ok {
		leftSchema, _ = e.engine.GetTable(scan.TableName)
	}
	if scan, ok := node.Right.(*plan.ScanNode); ok {
		rightSchema, _ = e.engine.GetTable(scan.TableName)
	}

	// Combined columns
	columns := append(leftResult.Columns, rightResult.Columns...)

	var resultRows []types.Row

	// Nested loop join
	for _, leftRow := range leftResult.Rows {
		matched := false
		for _, rightRow := range rightResult.Rows {
			combinedRow := append(append(types.Row{}, leftRow...), rightRow...)

			// Evaluate join predicate
			if node.Predicate != nil {
				eval := &JoinEvaluator{
					LeftRow:    leftRow,
					RightRow:   rightRow,
					LeftSchema: leftSchema,
					RightSchema: rightSchema,
				}
				val, err := eval.Evaluate(*node.Predicate)
				if err != nil {
					return nil, err
				}
				if !isTruthy(val) {
					continue
				}
			}

			resultRows = append(resultRows, combinedRow)
			matched = true
		}

		// Handle LEFT JOIN - add null row for unmatched left rows
		if node.JoinType == parser.JoinLeft && !matched {
			nullRightRow := make(types.Row, len(rightResult.Columns))
			combinedRow := append(append(types.Row{}, leftRow...), nullRightRow...)
			resultRows = append(resultRows, combinedRow)
		}
	}

	// Handle RIGHT JOIN - add null row for unmatched right rows
	if node.JoinType == parser.JoinRight {
		for _, rightRow := range rightResult.Rows {
			matched := false
			for _, leftRow := range leftResult.Rows {
				if node.Predicate != nil {
					eval := &JoinEvaluator{
						LeftRow:    leftRow,
						RightRow:   rightRow,
						LeftSchema: leftSchema,
						RightSchema: rightSchema,
					}
					val, err := eval.Evaluate(*node.Predicate)
					if err != nil {
						return nil, err
					}
					if isTruthy(val) {
						matched = true
						break
					}
				} else {
					matched = true
					break
				}
			}

			if !matched {
				nullLeftRow := make(types.Row, len(leftResult.Columns))
				combinedRow := append(append(types.Row{}, nullLeftRow...), rightRow...)
				resultRows = append(resultRows, combinedRow)
			}
		}
	}

	return &Result{Columns: columns, Rows: resultRows}, nil
}

// JoinEvaluator evaluates expressions in join context
type JoinEvaluator struct {
	LeftRow    types.Row
	RightRow   types.Row
	LeftSchema *types.Table
	RightSchema *types.Table
}

// Evaluate evaluates an expression
func (e *JoinEvaluator) Evaluate(expr parser.Expression) (types.Value, error) {
	switch ex := expr.(type) {
	case *parser.LiteralExpression:
		return ex.Value, nil

	case *parser.IdentifierExpression:
		// Try left schema first
		if e.LeftSchema != nil {
			if idx, ok := e.LeftSchema.GetColumnIndex(ex.Name); ok {
				return e.LeftRow[idx], nil
			}
		}
		// Try right schema
		if e.RightSchema != nil {
			if idx, ok := e.RightSchema.GetColumnIndex(ex.Name); ok {
				return e.RightRow[idx], nil
			}
		}
		return types.Value{}, dberror.NewColumnNotFoundError(ex.Name)

	case *parser.QualifiedIdentifierExpression:
		// Check table name to determine which schema to use
		if e.LeftSchema != nil && ex.Table == e.LeftSchema.Name {
			if idx, ok := e.LeftSchema.GetColumnIndex(ex.Column); ok {
				return e.LeftRow[idx], nil
			}
		}
		if e.RightSchema != nil && ex.Table == e.RightSchema.Name {
			if idx, ok := e.RightSchema.GetColumnIndex(ex.Column); ok {
				return e.RightRow[idx], nil
			}
		}
		return types.Value{}, dberror.NewColumnNotFoundError(ex.Table + "." + ex.Column)

	case *parser.BinaryOperation:
		left, err := e.Evaluate(ex.Left)
		if err != nil {
			return types.Value{}, err
		}
		right, err := e.Evaluate(ex.Right)
		if err != nil {
			return types.Value{}, err
		}
		return e.evaluateBinaryOp(left, ex.Op, right)

	case *parser.UnaryOperation:
		val, err := e.Evaluate(ex.Expr)
		if err != nil {
			return types.Value{}, err
		}
		return e.evaluateUnaryOp(ex.Op, val)

	default:
		return types.Value{}, dberror.NewInternalError("unsupported expression type in join")
	}
}

func (e *JoinEvaluator) evaluateBinaryOp(left types.Value, op parser.BinaryOp, right types.Value) (types.Value, error) {
	if left.Null || right.Null {
		switch op {
		case parser.OpEq, parser.OpNe, parser.OpLt, parser.OpLe, parser.OpGt, parser.OpGe:
			return types.NewNullValue(), nil
		}
	}

	switch op {
	case parser.OpEq:
		return types.NewBoolValue(left.Compare(right) == 0), nil
	case parser.OpNe:
		return types.NewBoolValue(left.Compare(right) != 0), nil
	case parser.OpLt:
		return types.NewBoolValue(left.Compare(right) < 0), nil
	case parser.OpLe:
		return types.NewBoolValue(left.Compare(right) <= 0), nil
	case parser.OpGt:
		return types.NewBoolValue(left.Compare(right) > 0), nil
	case parser.OpGe:
		return types.NewBoolValue(left.Compare(right) >= 0), nil
	case parser.OpAnd:
		return types.NewBoolValue(isTruthy(left) && isTruthy(right)), nil
	case parser.OpOr:
		return types.NewBoolValue(isTruthy(left) || isTruthy(right)), nil
	default:
		return types.Value{}, dberror.NewInternalError("unsupported binary operator")
	}
}

func (e *JoinEvaluator) evaluateUnaryOp(op parser.UnaryOp, val types.Value) (types.Value, error) {
	switch op {
	case parser.OpNot:
		return types.NewBoolValue(!isTruthy(val)), nil
	default:
		return types.Value{}, dberror.NewInternalError("unsupported unary operator")
	}
}
