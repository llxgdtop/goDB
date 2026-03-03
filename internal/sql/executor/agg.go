package executor

import (
	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// AggregateGroup represents a group of rows for aggregation
type AggregateGroup struct {
	Key   []types.Value
	Rows  []types.Row
}

func (e *Executor) executeAggregate(node *plan.AggregateNode) (*Result, error) {
	sourceResult, err := e.Execute(node.Source)
	if err != nil {
		return nil, err
	}

	// Get schema info
	var schema *types.Table
	if scan, ok := node.Source.(*plan.ScanNode); ok {
		schema, _ = e.engine.GetTable(scan.TableName)
	}

	// Group rows
	groups := make(map[string]*AggregateGroup)
	var groupKeys []string

	for _, row := range sourceResult.Rows {
		var key []types.Value
		var keyStr string

		if len(node.GroupBy) > 0 {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			for _, expr := range node.GroupBy {
				val, err := eval.Evaluate(expr)
				if err != nil {
					return nil, err
				}
				key = append(key, val)
				keyStr += val.String() + "|"
			}
		} else {
			keyStr = "__all__"
		}

		if _, exists := groups[keyStr]; !exists {
			groups[keyStr] = &AggregateGroup{Key: key, Rows: []types.Row{}}
			groupKeys = append(groupKeys, keyStr)
		}
		groups[keyStr].Rows = append(groups[keyStr].Rows, row)
	}

	// Compute aggregates for each group
	var columns []string
	var resultRows []types.Row

	for _, expr := range node.Exprs {
		switch ex := expr.Expr.(type) {
		case *parser.StarExpression:
			// Include all columns from source
			columns = append(columns, sourceResult.Columns...)
		case *parser.IdentifierExpression:
			columns = append(columns, ex.Name)
		case *parser.QualifiedIdentifierExpression:
			columns = append(columns, ex.Column)
		case *parser.FunctionCall:
			name := ex.Name
			if expr.As != "" {
				name = expr.As
			}
			columns = append(columns, name)
		default:
			if expr.As != "" {
				columns = append(columns, expr.As)
			}
		}
	}

	for _, groupKey := range groupKeys {
		group := groups[groupKey]
		var resultRow types.Row

		for _, expr := range node.Exprs {
			switch ex := expr.Expr.(type) {
			case *parser.StarExpression:
				// Include first row values
				if len(group.Rows) > 0 {
					resultRow = append(resultRow, group.Rows[0]...)
				}
			case *parser.FunctionCall:
				val, err := e.computeAggregate(ex, group.Rows, schema)
				if err != nil {
					return nil, err
				}
				resultRow = append(resultRow, val)
			case *parser.IdentifierExpression:
				// Group by column - use the key value
				if len(group.Key) > 0 {
					// Find which group by column this is
					for i, gbExpr := range node.GroupBy {
						if id, ok := gbExpr.(*parser.IdentifierExpression); ok && id.Name == ex.Name {
							resultRow = append(resultRow, group.Key[i])
							break
						}
					}
				} else if len(group.Rows) > 0 {
					// No group by - use first row
					eval := &ExpressionEvaluator{Row: group.Rows[0], Schema: schema}
					val, err := eval.Evaluate(ex)
					if err != nil {
						return nil, err
					}
					resultRow = append(resultRow, val)
				}
			default:
				// Evaluate expression for first row in group
				if len(group.Rows) > 0 {
					eval := &ExpressionEvaluator{Row: group.Rows[0], Schema: schema}
					val, err := eval.Evaluate(expr.Expr)
					if err != nil {
						return nil, err
					}
					resultRow = append(resultRow, val)
				}
			}
		}

		resultRows = append(resultRows, resultRow)
	}

	return &Result{Columns: columns, Rows: resultRows}, nil
}

func (e *Executor) computeAggregate(fc *parser.FunctionCall, rows []types.Row, schema *types.Table) (types.Value, error) {
	switch fc.Name {
	case "COUNT":
		if _, ok := fc.Arg.(*parser.StarExpression); ok {
			return types.NewIntValue(int64(len(rows))), nil
		}
		// Count non-null values
		count := int64(0)
		for _, row := range rows {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			val, err := eval.Evaluate(fc.Arg)
			if err != nil {
				return types.Value{}, err
			}
			if !val.Null {
				count++
			}
		}
		return types.NewIntValue(count), nil

	case "SUM":
		var sum float64
		hasValue := false
		for _, row := range rows {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			val, err := eval.Evaluate(fc.Arg)
			if err != nil {
				return types.Value{}, err
			}
			if !val.Null {
				sum += asFloat(val)
				hasValue = true
			}
		}
		if !hasValue {
			return types.NewNullValue(), nil
		}
		return types.NewFloatValue(sum), nil

	case "AVG":
		var sum float64
		var count int64
		for _, row := range rows {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			val, err := eval.Evaluate(fc.Arg)
			if err != nil {
				return types.Value{}, err
			}
			if !val.Null {
				sum += asFloat(val)
				count++
			}
		}
		if count == 0 {
			return types.NewNullValue(), nil
		}
		return types.NewFloatValue(sum / float64(count)), nil

	case "MIN":
		min := types.NewNullValue()
		for _, row := range rows {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			val, err := eval.Evaluate(fc.Arg)
			if err != nil {
				return types.Value{}, err
			}
			if !val.Null {
				if min.Null || val.Compare(min) < 0 {
					min = val
				}
			}
		}
		if min.Null {
			return types.NewNullValue(), nil
		}
		return min, nil

	case "MAX":
		max := types.NewNullValue()
		for _, row := range rows {
			eval := &ExpressionEvaluator{Row: row, Schema: schema}
			val, err := eval.Evaluate(fc.Arg)
			if err != nil {
				return types.Value{}, err
			}
			if !val.Null {
				if max.Null || val.Compare(max) > 0 {
					max = val
				}
			}
		}
		if max.Null {
			return types.NewNullValue(), nil
		}
		return max, nil

	default:
		return types.Value{}, dberror.NewInternalError("unknown aggregate function: " + fc.Name)
	}
}
