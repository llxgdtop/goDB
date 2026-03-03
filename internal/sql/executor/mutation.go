package executor

import (
	"fmt"

	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

func (e *Executor) executeInsert(node *plan.InsertNode) (*Result, error) {
	table, ok := e.engine.GetTable(node.TableName)
	if !ok {
		return nil, dberror.NewTableNotFoundError(node.TableName)
	}

	// Get primary key index for duplicate checking
	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return nil, dberror.NewInternalError("table has no primary key")
	}

	// Determine which columns are being inserted
	var targetIndices []int
	if len(node.Columns) > 0 {
		for _, colName := range node.Columns {
			idx, ok := table.GetColumnIndex(colName)
			if !ok {
				return nil, dberror.NewColumnNotFoundError(colName)
			}
			targetIndices = append(targetIndices, idx)
		}
	} else {
		// All columns in order
		for i := range table.Columns {
			targetIndices = append(targetIndices, i)
		}
	}

	// Build all rows first
	var rows []types.Row
	for _, valueRow := range node.Values {
		// Validate value count
		if len(valueRow) != len(targetIndices) {
			return nil, dberror.NewInternalError("column count doesn't match value count")
		}

		row := make(types.Row, len(table.Columns))

		// Evaluate values and assign to row
		for i, expr := range valueRow {
			val, err := e.evaluateConstant(expr, table.Columns[targetIndices[i]])
			if err != nil {
				return nil, err
			}
			row[targetIndices[i]] = val
		}

		// Apply defaults for unspecified columns
		for i, col := range table.Columns {
			if row[i].Null && col.Default != nil {
				row[i] = *col.Default
			}
		}

		// Validate NOT NULL constraints
		for i, col := range table.Columns {
			if !col.Nullable && row[i].Null {
				return nil, dberror.NewInternalError("column %s cannot be null", col.Name)
			}
		}

		rows = append(rows, row)
	}

	// For multi-row INSERT, check ALL rows for duplicates BEFORE inserting ANY
	// This includes checking against:
	// 1. Already committed data
	// 2. Rows already in the current transaction
	// 3. Other rows in the same INSERT statement
	if len(rows) > 1 {
		// First, check for duplicates within the same INSERT statement
		seenKeys := make(map[interface{}]bool)
		for _, row := range rows {
			pkValue := row[pkIdx]
			var key interface{}
			switch pkValue.Type {
			case types.TypeInteger:
				key = pkValue.Int
			case types.TypeString:
				key = pkValue.Str
			case types.TypeFloat:
				key = pkValue.Float
			case types.TypeBoolean:
				key = pkValue.Bool
			}
			if seenKeys[key] {
				return nil, dberror.NewDuplicateKeyError()
			}
			seenKeys[key] = true
		}

		// Then, check for duplicates against existing data (committed + transaction buffer)
		for _, row := range rows {
			if err := e.engine.CheckDuplicate(node.TableName, row); err != nil {
				return nil, err
			}
		}
	}

	// For multi-row INSERT in auto-commit mode, wrap in a transaction
	needsTransaction := len(rows) > 1 && !e.engine.IsInTransaction()

	if needsTransaction {
		if err := e.engine.Begin(); err != nil {
			return nil, err
		}
	}

	var insertedCount int64

	// Now insert all rows
	for _, row := range rows {
		if err := e.engine.Insert(node.TableName, node.Columns, row); err != nil {
			if needsTransaction {
				e.engine.Rollback()
			}
			return nil, err
		}
		insertedCount++
	}

	// Commit the transaction if we started one
	if needsTransaction {
		if err := e.engine.Commit(); err != nil {
			return nil, err
		}
	}

	return &Result{Message: fmt.Sprintf("INSERT %d rows", insertedCount)}, nil
}

func (e *Executor) evaluateConstant(expr parser.Expression, col types.Column) (types.Value, error) {
	switch ex := expr.(type) {
	case *parser.LiteralExpression:
		return ex.Value, nil
	case *parser.IdentifierExpression:
		// Handle NULL keyword
		if ex.Name == "NULL" {
			return types.NewNullValue(), nil
		}
		return types.Value{}, dberror.NewInternalError("unexpected identifier: " + ex.Name)
	default:
		return types.Value{}, dberror.NewInternalError("expected constant expression")
	}
}

func (e *Executor) executeUpdate(node *plan.UpdateNode) (*Result, error) {
	table, ok := e.engine.GetTable(node.TableName)
	if !ok {
		return nil, dberror.NewTableNotFoundError(node.TableName)
	}

	// Build filter function
	var filterFunc func(types.Row) bool
	if scan, ok := node.Source.(*plan.ScanNode); ok && scan.Filter != nil {
		filterFunc = func(row types.Row) bool {
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(*scan.Filter)
			if err != nil {
				return false
			}
			return isTruthy(val)
		}
	} else if filter, ok := node.Source.(*plan.FilterNode); ok {
		filterFunc = func(row types.Row) bool {
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(filter.Predicate)
			if err != nil {
				return false
			}
			return isTruthy(val)
		}
	}

	// Build update function
	updateFunc := func(row types.Row) types.Row {
		newRow := make(types.Row, len(row))
		copy(newRow, row)

		for colName, expr := range node.Columns {
			idx, ok := table.GetColumnIndex(colName)
			if !ok {
				return row
			}
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(expr)
			if err != nil {
				return row
			}
			newRow[idx] = val
		}

		return newRow
	}

	count, err := e.engine.Update(node.TableName, filterFunc, updateFunc)
	if err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("UPDATE %d rows", count)}, nil
}

func (e *Executor) executeDelete(node *plan.DeleteNode) (*Result, error) {
	table, ok := e.engine.GetTable(node.TableName)
	if !ok {
		return nil, dberror.NewTableNotFoundError(node.TableName)
	}

	// Build filter function
	var filterFunc func(types.Row) bool
	if scan, ok := node.Source.(*plan.ScanNode); ok && scan.Filter != nil {
		filterFunc = func(row types.Row) bool {
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(*scan.Filter)
			if err != nil {
				return false
			}
			return isTruthy(val)
		}
	} else if filter, ok := node.Source.(*plan.FilterNode); ok {
		filterFunc = func(row types.Row) bool {
			eval := &ExpressionEvaluator{Row: row, Schema: table}
			val, err := eval.Evaluate(filter.Predicate)
			if err != nil {
				return false
			}
			return isTruthy(val)
		}
	}

	count, err := e.engine.Delete(node.TableName, filterFunc)
	if err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("DELETE %d rows", count)}, nil
}
