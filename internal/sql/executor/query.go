package executor

import (
	"sort"

	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// ExpressionEvaluator evaluates expressions
type ExpressionEvaluator struct {
	Row    types.Row
	Schema *types.Table
}

// Evaluate evaluates an expression
func (e *ExpressionEvaluator) Evaluate(expr parser.Expression) (types.Value, error) {
	switch ex := expr.(type) {
	case *parser.LiteralExpression:
		return ex.Value, nil

	case *parser.IdentifierExpression:
		idx, ok := e.Schema.GetColumnIndex(ex.Name)
		if !ok {
			return types.Value{}, dberror.NewColumnNotFoundError(ex.Name)
		}
		return e.Row[idx], nil

	case *parser.QualifiedIdentifierExpression:
		idx, ok := e.Schema.GetColumnIndex(ex.Column)
		if !ok {
			return types.Value{}, dberror.NewColumnNotFoundError(ex.Column)
		}
		return e.Row[idx], nil

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

	case *parser.StarExpression:
		return types.NewIntValue(1), nil // For COUNT(*)

	case *parser.IsNullExpression:
		val, err := e.Evaluate(ex.Expr)
		if err != nil {
			return types.Value{}, err
		}
		return types.NewBoolValue(val.Null == ex.Not), nil

	default:
		return types.Value{}, dberror.NewInternalError("unsupported expression type")
	}
}

func (e *ExpressionEvaluator) evaluateBinaryOp(left types.Value, op parser.BinaryOp, right types.Value) (types.Value, error) {
	// Handle NULL
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
	case parser.OpAdd:
		if left.Type == types.TypeInteger && right.Type == types.TypeInteger {
			return types.NewIntValue(left.Int + right.Int), nil
		}
		return types.NewFloatValue(asFloat(left) + asFloat(right)), nil
	case parser.OpSub:
		if left.Type == types.TypeInteger && right.Type == types.TypeInteger {
			return types.NewIntValue(left.Int - right.Int), nil
		}
		return types.NewFloatValue(asFloat(left) - asFloat(right)), nil
	case parser.OpMul:
		if left.Type == types.TypeInteger && right.Type == types.TypeInteger {
			return types.NewIntValue(left.Int * right.Int), nil
		}
		return types.NewFloatValue(asFloat(left) * asFloat(right)), nil
	case parser.OpDiv:
		if left.Type == types.TypeInteger && right.Type == types.TypeInteger {
			if right.Int == 0 {
				return types.NewNullValue(), nil
			}
			return types.NewIntValue(left.Int / right.Int), nil
		}
		if asFloat(right) == 0 {
			return types.NewNullValue(), nil
		}
		return types.NewFloatValue(asFloat(left) / asFloat(right)), nil
	default:
		return types.Value{}, dberror.NewInternalError("unsupported binary operator")
	}
}

func (e *ExpressionEvaluator) evaluateUnaryOp(op parser.UnaryOp, val types.Value) (types.Value, error) {
	switch op {
	case parser.OpNot:
		return types.NewBoolValue(!isTruthy(val)), nil
	case parser.OpNeg:
		if val.Type == types.TypeInteger {
			return types.NewIntValue(-val.Int), nil
		}
		return types.NewFloatValue(-asFloat(val)), nil
	default:
		return types.Value{}, dberror.NewInternalError("unsupported unary operator")
	}
}

func isTruthy(v types.Value) bool {
	if v.Null {
		return false
	}
	switch v.Type {
	case types.TypeBoolean:
		return v.Bool
	case types.TypeInteger:
		return v.Int != 0
	case types.TypeFloat:
		return v.Float != 0
	case types.TypeString:
		return v.Str != ""
	default:
		return false
	}
}

func asFloat(v types.Value) float64 {
	switch v.Type {
	case types.TypeInteger:
		return float64(v.Int)
	case types.TypeFloat:
		return v.Float
	default:
		return 0
	}
}

func (e *Executor) executeScan(node *plan.ScanNode) (*Result, error) {
	table, ok := e.engine.GetTable(node.TableName)
	if !ok {
		return nil, dberror.NewTableNotFoundError(node.TableName)
	}

	var filterFunc func(types.Row) bool
	if node.Filter != nil {
		filterFunc = func(row types.Row) bool {
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(*node.Filter)
			if err != nil {
				return false
			}
			return isTruthy(val)
		}
	}

	rows, err := e.engine.Scan(node.TableName, filterFunc)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = col.Name
	}

	return &Result{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeProjection(node *plan.ProjectionNode) (*Result, error) {
	var sourceResult *Result
	var err error

	if node.Source != nil {
		sourceResult, err = e.Execute(node.Source)
		if err != nil {
			return nil, err
		}
	} else {
		sourceResult = &Result{}
	}

	// Check if source is an aggregate node - if so, just select columns by position
	// because aggregates have already been computed
	if _, isAggregate := node.Source.(*plan.AggregateNode); isAggregate {
		return e.executeProjectionFromAggregate(node, sourceResult)
	}

	// Get schema info - recursively find underlying ScanNode
	var schema *types.Table
	scan := findScanNode(node.Source)
	if scan != nil {
		schema, _ = e.engine.GetTable(scan.TableName)
	}

	var columns []string
	var rows []types.Row

	for _, expr := range node.Exprs {
		switch ex := expr.Expr.(type) {
		case *parser.StarExpression:
			// Include all columns
			if sourceResult.Columns != nil {
				columns = append(columns, sourceResult.Columns...)
			}
		default:
			columns = append(columns, expr.As)
			if expr.As == "" {
				// Try to get name from expression
				if id, ok := ex.(*parser.IdentifierExpression); ok {
					columns[len(columns)-1] = id.Name
				} else if qid, ok := ex.(*parser.QualifiedIdentifierExpression); ok {
					columns[len(columns)-1] = qid.Column
				} else if fc, ok := ex.(*parser.FunctionCall); ok {
					columns[len(columns)-1] = fc.Name + "()"
				}
			}
		}
	}

	// If no rows, just return columns
	if len(sourceResult.Rows) == 0 {
		return &Result{Columns: columns, Rows: rows}, nil
	}

	// Project each row
	for _, row := range sourceResult.Rows {
		var projectedRow types.Row
		for _, expr := range node.Exprs {
			if _, ok := expr.Expr.(*parser.StarExpression); ok {
				projectedRow = append(projectedRow, row...)
			} else {
				eval := &ExpressionEvaluator{Row: row, Schema: schema}
				val, err := eval.Evaluate(expr.Expr)
				if err != nil {
					return nil, err
				}
				projectedRow = append(projectedRow, val)
			}
		}
		rows = append(rows, projectedRow)
	}

	return &Result{Columns: columns, Rows: rows}, nil
}

// executeProjectionFromAggregate handles projection when source is an aggregate
// In this case, we select columns by position from the already-computed aggregate result
func (e *Executor) executeProjectionFromAggregate(node *plan.ProjectionNode, sourceResult *Result) (*Result, error) {
	var columns []string
	var selectedIndices []int

	for _, expr := range node.Exprs {
		switch ex := expr.Expr.(type) {
		case *parser.StarExpression:
			// Include all columns from source
			for i, col := range sourceResult.Columns {
				columns = append(columns, col)
				selectedIndices = append(selectedIndices, i)
			}
		case *parser.IdentifierExpression:
			// Find column by name
			colName := expr.As
			if colName == "" {
				colName = ex.Name
			}
			idx := -1
			for i, c := range sourceResult.Columns {
				if c == colName {
					idx = i
					break
				}
			}
			if idx == -1 {
				// Try to find by original expression match
				for i, c := range sourceResult.Columns {
					if c == ex.Name {
						idx = i
						break
					}
				}
			}
			if idx >= 0 {
				columns = append(columns, colName)
				selectedIndices = append(selectedIndices, idx)
			}
		case *parser.FunctionCall:
			// Find column by alias or function name
			colName := expr.As
			if colName == "" {
				colName = ex.Name + "()"
			}
			idx := -1
			for i, c := range sourceResult.Columns {
				if c == colName || c == expr.As || c == ex.Name {
					idx = i
					break
				}
			}
			if idx >= 0 {
				columns = append(columns, colName)
				selectedIndices = append(selectedIndices, idx)
			}
		default:
			// For other expressions, try to match by alias
			if expr.As != "" {
				idx := -1
				for i, c := range sourceResult.Columns {
					if c == expr.As {
						idx = i
						break
					}
				}
				if idx >= 0 {
					columns = append(columns, expr.As)
					selectedIndices = append(selectedIndices, idx)
				}
			}
		}
	}

	// If no columns matched, use all source columns
	if len(columns) == 0 {
		columns = sourceResult.Columns
		for i := range sourceResult.Columns {
			selectedIndices = append(selectedIndices, i)
		}
	}

	// Project rows
	var rows []types.Row
	for _, row := range sourceResult.Rows {
		var projectedRow types.Row
		for _, idx := range selectedIndices {
			if idx < len(row) {
				projectedRow = append(projectedRow, row[idx])
			}
		}
		rows = append(rows, projectedRow)
	}

	return &Result{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executeOrder(node *plan.OrderNode) (*Result, error) {
	sourceResult, err := e.Execute(node.Source)
	if err != nil {
		return nil, err
	}

	// Get schema info - recursively find underlying ScanNode
	var schema *types.Table
	scan := findScanNode(node.Source)
	if scan != nil {
		schema, _ = e.engine.GetTable(scan.TableName)
	}

	rows := make([]types.Row, len(sourceResult.Rows))
	copy(rows, sourceResult.Rows)

	sort.Slice(rows, func(i, j int) bool {
		for _, item := range node.OrderBy {
			evalI := &ExpressionEvaluator{Row: rows[i], Schema: schema}
			evalJ := &ExpressionEvaluator{Row: rows[j], Schema: schema}

			valI, err := evalI.Evaluate(item.Expr)
			if err != nil {
				return false
			}
			valJ, err := evalJ.Evaluate(item.Expr)
			if err != nil {
				return false
			}

			cmp := valI.Compare(valJ)
			if cmp == 0 {
				continue
			}

			if item.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return &Result{Columns: sourceResult.Columns, Rows: rows}, nil
}

func (e *Executor) executeLimit(node *plan.LimitNode) (*Result, error) {
	sourceResult, err := e.Execute(node.Source)
	if err != nil {
		return nil, err
	}

	if int(node.Limit) >= len(sourceResult.Rows) {
		return sourceResult, nil
	}

	return &Result{
		Columns: sourceResult.Columns,
		Rows:    sourceResult.Rows[:node.Limit],
	}, nil
}

func (e *Executor) executeOffset(node *plan.OffsetNode) (*Result, error) {
	sourceResult, err := e.Execute(node.Source)
	if err != nil {
		return nil, err
	}

	if int(node.Offset) >= len(sourceResult.Rows) {
		return &Result{Columns: sourceResult.Columns, Rows: nil}, nil
	}

	return &Result{
		Columns: sourceResult.Columns,
		Rows:    sourceResult.Rows[node.Offset:],
	}, nil
}

func (e *Executor) executeFilter(node *plan.FilterNode) (*Result, error) {
	sourceResult, err := e.Execute(node.Source)
	if err != nil {
		return nil, err
	}

	// Get schema info - recursively find underlying ScanNode
	var schema *types.Table
	scan := findScanNode(node.Source)
	if scan != nil {
		schema, _ = e.engine.GetTable(scan.TableName)
	}

	var filteredRows []types.Row
	for _, row := range sourceResult.Rows {
		eval := &ExpressionEvaluator{Row: row, Schema: schema}
		val, err := eval.Evaluate(node.Predicate)
		if err != nil {
			return nil, err
		}
		if isTruthy(val) {
			filteredRows = append(filteredRows, row)
		}
	}

	return &Result{Columns: sourceResult.Columns, Rows: filteredRows}, nil
}

// findScanNode recursively finds the underlying ScanNode from a plan node
func findScanNode(node plan.Node) *plan.ScanNode {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plan.ScanNode:
		return n
	case *plan.FilterNode:
		return findScanNode(n.Source)
	case *plan.ProjectionNode:
		return findScanNode(n.Source)
	case *plan.OrderNode:
		return findScanNode(n.Source)
	case *plan.LimitNode:
		return findScanNode(n.Source)
	case *plan.OffsetNode:
		return findScanNode(n.Source)
	case *plan.AggregateNode:
		return findScanNode(n.Source)
	default:
		return nil
	}
}
