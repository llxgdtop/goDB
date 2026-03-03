package executor

import (
	"fmt"
	"strings"

	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

func (e *Executor) executeCreateTable(node *plan.CreateTableNode) (*Result, error) {
	if err := e.engine.CreateTable(node.Schema); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", node.Schema.Name)}, nil
}

func (e *Executor) executeBegin(node *plan.BeginNode) (*Result, error) {
	if err := e.engine.Begin(); err != nil {
		return nil, err
	}
	version := e.engine.GetTransactionVersion()
	return &Result{Message: fmt.Sprintf("TRANSACTION %d BEGIN", version)}, nil
}

func (e *Executor) executeCommit(node *plan.CommitNode) (*Result, error) {
	version := e.engine.GetTransactionVersion()
	if err := e.engine.Commit(); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("TRANSACTION %d COMMIT", version)}, nil
}

func (e *Executor) executeRollback(node *plan.RollbackNode) (*Result, error) {
	version := e.engine.GetTransactionVersion()
	if err := e.engine.Rollback(); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("TRANSACTION %d ROLLBACK", version)}, nil
}

func (e *Executor) executeShowTables(node *plan.ShowTablesNode) (*Result, error) {
	tables := e.engine.GetTables()
	var names []string
	for name := range tables {
		names = append(names, name)
	}
	return &Result{
		Columns: []string{"table_name"},
		Rows:    rowsFromStrings(names),
	}, nil
}

func (e *Executor) executeShowTable(node *plan.ShowTableNode) (*Result, error) {
	table, ok := e.engine.GetTable(node.Name)
	if !ok {
		return nil, fmt.Errorf("table %s not found", node.Name)
	}

	var columns []string
	var rows []types.Row

	for _, col := range table.Columns {
		columns = []string{"column_name", "type", "nullable", "primary_key"}
		row := types.Row{
			types.NewStringValue(col.Name),
			types.NewStringValue(col.DataType.String()),
			types.NewBoolValue(col.Nullable),
			types.NewBoolValue(col.PrimaryKey),
		}
		rows = append(rows, row)
	}

	return &Result{Columns: columns, Rows: rows}, nil
}

func rowsFromStrings(strs []string) []types.Row {
	var rows []types.Row
	for _, s := range strs {
		rows = append(rows, types.Row{types.NewStringValue(s)})
	}
	return rows
}

// FormatResult formats a result for display
func FormatResult(result *Result) string {
	if result == nil {
		return ""
	}

	if result.Message != "" {
		return result.Message
	}

	if len(result.Rows) == 0 {
		return "Empty set"
	}

	// Calculate column widths
	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col)
	}
	for _, row := range result.Rows {
		for i, val := range row {
			if len(val.String()) > widths[i] {
				widths[i] = len(val.String())
			}
		}
	}

	// Build output
	var lines []string

	// Header
	header := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		header[i] = fmt.Sprintf("%-*s", widths[i], col)
	}
	lines = append(lines, strings.Join(header, " |"))

	// Separator
	sep := make([]string, len(result.Columns))
	for i, w := range widths {
		sep[i] = strings.Repeat("-", w+1)
	}
	lines = append(lines, strings.Join(sep, "+"))

	// Rows
	for _, row := range result.Rows {
		rowStrs := make([]string, len(row))
		for i, val := range row {
			rowStrs[i] = fmt.Sprintf("%-*s", widths[i], val.String())
		}
		lines = append(lines, strings.Join(rowStrs, " |"))
	}

	// Footer (with parentheses like RustDB)
	lines = append(lines, fmt.Sprintf("(%d rows)", len(result.Rows)))

	return strings.Join(lines, "\n")
}
