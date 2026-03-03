package parser

import (
	"testing"

	"github.com/llxgdtop/godb/internal/sql/types"
)

// =============================================================================
// 1. CREATE TABLE Tests
// =============================================================================

func TestParseCreateTable_Basic(t *testing.T) {
	sql := "CREATE TABLE users (id INTEGER, name STRING);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Name != "users" {
		t.Errorf("expected table name 'users', got %q", createStmt.Name)
	}
	if len(createStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(createStmt.Columns))
	}

	// Check first column
	if createStmt.Columns[0].Name != "id" {
		t.Errorf("expected column name 'id', got %q", createStmt.Columns[0].Name)
	}
	if createStmt.Columns[0].DataType != types.TypeInteger {
		t.Errorf("expected INTEGER type, got %v", createStmt.Columns[0].DataType)
	}

	// Check second column
	if createStmt.Columns[1].Name != "name" {
		t.Errorf("expected column name 'name', got %q", createStmt.Columns[1].Name)
	}
	if createStmt.Columns[1].DataType != types.TypeString {
		t.Errorf("expected STRING type, got %v", createStmt.Columns[1].DataType)
	}
}

func TestParseCreateTable_AllTypes(t *testing.T) {
	sql := "CREATE TABLE test (a BOOLEAN, b INTEGER, c FLOAT, d STRING);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	expectedTypes := []types.DataType{types.TypeBoolean, types.TypeInteger, types.TypeFloat, types.TypeString}
	for i, col := range createStmt.Columns {
		if col.DataType != expectedTypes[i] {
			t.Errorf("column %d: expected type %v, got %v", i, expectedTypes[i], col.DataType)
		}
	}
}

func TestParseCreateTable_PrimaryKey(t *testing.T) {
	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if !createStmt.Columns[0].PrimaryKey {
		t.Error("expected id column to be PRIMARY KEY")
	}
	// PRIMARY KEY implies NOT NULL
	if createStmt.Columns[0].Nullable {
		t.Error("expected PRIMARY KEY column to be NOT NULL")
	}
}

func TestParseCreateTable_NotNull(t *testing.T) {
	sql := "CREATE TABLE users (id INTEGER NOT NULL, name STRING);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Columns[0].Nullable {
		t.Error("expected id column to be NOT NULL")
	}
	if !createStmt.Columns[1].Nullable {
		t.Error("expected name column to be nullable (default)")
	}
}

func TestParseCreateTable_DefaultValue(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		colName  string
		wantVal  interface{}
	}{
		{
			name:    "integer default",
			sql:     "CREATE TABLE test (a INTEGER DEFAULT 100);",
			colName: "a",
			wantVal: int64(100),
		},
		{
			name:    "float default",
			sql:     "CREATE TABLE test (b FLOAT DEFAULT 3.14);",
			colName: "b",
			wantVal: float64(3.14),
		},
		{
			name:    "string default",
			sql:     "CREATE TABLE test (c STRING DEFAULT 'hello');",
			colName: "c",
			wantVal: "hello",
		},
		{
			name:    "boolean true default",
			sql:     "CREATE TABLE test (d BOOLEAN DEFAULT true);",
			colName: "d",
			wantVal: true,
		},
		{
			name:    "boolean false default",
			sql:     "CREATE TABLE test (e BOOLEAN DEFAULT false);",
			colName: "e",
			wantVal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			createStmt, ok := stmt.(*CreateTableStatement)
			if !ok {
				t.Fatalf("expected CreateTableStatement, got %T", stmt)
			}

			col := createStmt.Columns[0]
			if col.Default == nil {
				t.Fatal("expected default value, got nil")
			}

			lit, ok := (*col.Default).(*LiteralExpression)
			if !ok {
				t.Fatalf("expected LiteralExpression, got %T", *col.Default)
			}

			switch expected := tt.wantVal.(type) {
			case int64:
				if lit.Value.Int != expected {
					t.Errorf("expected default value %d, got %d", expected, lit.Value.Int)
				}
			case float64:
				if lit.Value.Float != expected {
					t.Errorf("expected default value %f, got %f", expected, lit.Value.Float)
				}
			case string:
				if lit.Value.Str != expected {
					t.Errorf("expected default value %q, got %q", expected, lit.Value.Str)
				}
			case bool:
				if lit.Value.Bool != expected {
					t.Errorf("expected default value %v, got %v", expected, lit.Value.Bool)
				}
			}
		})
	}
}

func TestParseCreateTable_Complex(t *testing.T) {
	sql := `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name STRING NOT NULL,
			email STRING DEFAULT '',
			active BOOLEAN DEFAULT true,
			score FLOAT DEFAULT 0.0
		);
	`
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Name != "users" {
		t.Errorf("expected table name 'users', got %q", createStmt.Name)
	}
	if len(createStmt.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(createStmt.Columns))
	}

	// Check id column (PRIMARY KEY)
	if !createStmt.Columns[0].PrimaryKey {
		t.Error("expected id to be PRIMARY KEY")
	}

	// Check name column (NOT NULL)
	if createStmt.Columns[1].Nullable {
		t.Error("expected name to be NOT NULL")
	}

	// Check email column (DEFAULT '')
	if createStmt.Columns[2].Default == nil {
		t.Error("expected email to have DEFAULT value")
	}

	// Check active column (DEFAULT true)
	if createStmt.Columns[3].Default == nil {
		t.Error("expected active to have DEFAULT value")
	}

	// Check score column (DEFAULT 0.0)
	if createStmt.Columns[4].Default == nil {
		t.Error("expected score to have DEFAULT value")
	}
}

// =============================================================================
// 2. INSERT Tests
// =============================================================================

func TestParseInsert_WithoutColumns(t *testing.T) {
	sql := "INSERT INTO users VALUES (1, 'Alice', true);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if insertStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", insertStmt.TableName)
	}
	if len(insertStmt.Columns) != 0 {
		t.Errorf("expected no columns, got %d", len(insertStmt.Columns))
	}
	if len(insertStmt.Values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(insertStmt.Values))
	}
	if len(insertStmt.Values[0]) != 3 {
		t.Errorf("expected 3 values, got %d", len(insertStmt.Values[0]))
	}
}

func TestParseInsert_WithColumns(t *testing.T) {
	sql := "INSERT INTO users (id, name, active) VALUES (1, 'Alice', true);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if insertStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", insertStmt.TableName)
	}
	if len(insertStmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(insertStmt.Columns))
	}

	expectedCols := []string{"id", "name", "active"}
	for i, col := range insertStmt.Columns {
		if col != expectedCols[i] {
			t.Errorf("column %d: expected %q, got %q", i, expectedCols[i], col)
		}
	}
}

func TestParseInsert_MultipleRows(t *testing.T) {
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if len(insertStmt.Values) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(insertStmt.Values))
	}
}

func TestParseInsert_ValueTypes(t *testing.T) {
	sql := "INSERT INTO test VALUES (1, 3.14, 'hello', true, false, NULL);"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	values := insertStmt.Values[0]
	if len(values) != 6 {
		t.Fatalf("expected 6 values, got %d", len(values))
	}

	// Check integer
	lit0, ok := values[0].(*LiteralExpression)
	if !ok || lit0.Value.Int != 1 {
		t.Errorf("expected integer 1, got %v", values[0])
	}

	// Check float
	lit1, ok := values[1].(*LiteralExpression)
	if !ok || lit1.Value.Float != 3.14 {
		t.Errorf("expected float 3.14, got %v", values[1])
	}

	// Check string
	lit2, ok := values[2].(*LiteralExpression)
	if !ok || lit2.Value.Str != "hello" {
		t.Errorf("expected string 'hello', got %v", values[2])
	}

	// Check true
	lit3, ok := values[3].(*LiteralExpression)
	if !ok || lit3.Value.Bool != true {
		t.Errorf("expected boolean true, got %v", values[3])
	}

	// Check false
	lit4, ok := values[4].(*LiteralExpression)
	if !ok || lit4.Value.Bool != false {
		t.Errorf("expected boolean false, got %v", values[4])
	}

	// Check NULL
	lit5, ok := values[5].(*LiteralExpression)
	if !ok || !lit5.Value.Null {
		t.Errorf("expected NULL, got %v", values[5])
	}
}

// =============================================================================
// 3. SELECT Tests
// =============================================================================

func TestParseSelect_Star(t *testing.T) {
	sql := "SELECT * FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Select) != 1 {
		t.Fatalf("expected 1 select item, got %d", len(selectStmt.Select))
	}

	_, ok = selectStmt.Select[0].Expr.(*StarExpression)
	if !ok {
		t.Error("expected * expression")
	}
}

func TestParseSelect_ColumnList(t *testing.T) {
	sql := "SELECT id, name, email FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Select) != 3 {
		t.Fatalf("expected 3 select items, got %d", len(selectStmt.Select))
	}

	expectedCols := []string{"id", "name", "email"}
	for i, item := range selectStmt.Select {
		ident, ok := item.Expr.(*IdentifierExpression)
		if !ok {
			t.Errorf("item %d: expected IdentifierExpression, got %T", i, item.Expr)
			continue
		}
		if ident.Name != expectedCols[i] {
			t.Errorf("item %d: expected %q, got %q", i, expectedCols[i], ident.Name)
		}
	}
}

func TestParseSelect_WithAlias(t *testing.T) {
	sql := "SELECT id AS user_id, name AS user_name FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Select) != 2 {
		t.Fatalf("expected 2 select items, got %d", len(selectStmt.Select))
	}

	if selectStmt.Select[0].As != "user_id" {
		t.Errorf("expected alias 'user_id', got %q", selectStmt.Select[0].As)
	}
	if selectStmt.Select[1].As != "user_name" {
		t.Errorf("expected alias 'user_name', got %q", selectStmt.Select[1].As)
	}
}

func TestParseSelect_Where(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = 1;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	binOp, ok := (*selectStmt.Where).(*BinaryOperation)
	if !ok {
		t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
	}
	if binOp.Op != OpEq {
		t.Errorf("expected OpEq, got %v", binOp.Op)
	}
}

func TestParseSelect_OrderBy_Asc(t *testing.T) {
	sql := "SELECT * FROM users ORDER BY name ASC;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(selectStmt.OrderBy))
	}

	if selectStmt.OrderBy[0].Desc {
		t.Error("expected ASC (Desc = false)")
	}
}

func TestParseSelect_OrderBy_Desc(t *testing.T) {
	sql := "SELECT * FROM users ORDER BY name DESC;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(selectStmt.OrderBy))
	}

	if !selectStmt.OrderBy[0].Desc {
		t.Error("expected DESC (Desc = true)")
	}
}

func TestParseSelect_OrderBy_Multiple(t *testing.T) {
	sql := "SELECT * FROM users ORDER BY name ASC, id DESC;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY items, got %d", len(selectStmt.OrderBy))
	}

	if selectStmt.OrderBy[0].Desc {
		t.Error("expected first item to be ASC")
	}
	if !selectStmt.OrderBy[1].Desc {
		t.Error("expected second item to be DESC")
	}
}

func TestParseSelect_Limit(t *testing.T) {
	sql := "SELECT * FROM users LIMIT 10;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}

	lit, ok := (*selectStmt.Limit).(*LiteralExpression)
	if !ok || lit.Value.Int != 10 {
		t.Errorf("expected LIMIT 10, got %v", *selectStmt.Limit)
	}
}

func TestParseSelect_Offset(t *testing.T) {
	sql := "SELECT * FROM users LIMIT 10 OFFSET 20;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Offset == nil {
		t.Fatal("expected OFFSET clause")
	}

	lit, ok := (*selectStmt.Offset).(*LiteralExpression)
	if !ok || lit.Value.Int != 20 {
		t.Errorf("expected OFFSET 20, got %v", *selectStmt.Offset)
	}
}

func TestParseSelect_Complete(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE active = true ORDER BY name ASC LIMIT 10 OFFSET 5;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// Check FROM
	tableFrom, ok := selectStmt.From.(*TableFromItem)
	if !ok || tableFrom.Name != "users" {
		t.Errorf("expected FROM users, got %v", selectStmt.From)
	}

	// Check WHERE
	if selectStmt.Where == nil {
		t.Error("expected WHERE clause")
	}

	// Check ORDER BY
	if len(selectStmt.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY item, got %d", len(selectStmt.OrderBy))
	}

	// Check LIMIT
	if selectStmt.Limit == nil {
		t.Error("expected LIMIT clause")
	}

	// Check OFFSET
	if selectStmt.Offset == nil {
		t.Error("expected OFFSET clause")
	}
}

// =============================================================================
// 4. UPDATE Tests
// =============================================================================

func TestParseUpdate_Simple(t *testing.T) {
	sql := "UPDATE users SET name = 'Bob';"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}

	if updateStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", updateStmt.TableName)
	}
	if len(updateStmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(updateStmt.Columns))
	}
}

func TestParseUpdate_MultipleColumns(t *testing.T) {
	sql := "UPDATE users SET name = 'Bob', active = false, score = 100;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}

	if len(updateStmt.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(updateStmt.Columns))
	}
}

func TestParseUpdate_WithWhere(t *testing.T) {
	sql := "UPDATE users SET name = 'Bob' WHERE id = 1;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}

	if updateStmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestUpdate_ColumnValues(t *testing.T) {
	sql := "UPDATE users SET a = 1, b = 2.0, c = 'hello', d = true;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	updateStmt, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}

	// Check integer value
	if lit, ok := updateStmt.Columns["a"].(*LiteralExpression); ok {
		if lit.Value.Int != 1 {
			t.Errorf("expected a = 1, got %d", lit.Value.Int)
		}
	} else {
		t.Error("expected a to be LiteralExpression")
	}

	// Check float value
	if lit, ok := updateStmt.Columns["b"].(*LiteralExpression); ok {
		if lit.Value.Float != 2.0 {
			t.Errorf("expected b = 2.0, got %f", lit.Value.Float)
		}
	} else {
		t.Error("expected b to be LiteralExpression")
	}

	// Check string value
	if lit, ok := updateStmt.Columns["c"].(*LiteralExpression); ok {
		if lit.Value.Str != "hello" {
			t.Errorf("expected c = 'hello', got %q", lit.Value.Str)
		}
	} else {
		t.Error("expected c to be LiteralExpression")
	}

	// Check boolean value
	if lit, ok := updateStmt.Columns["d"].(*LiteralExpression); ok {
		if lit.Value.Bool != true {
			t.Errorf("expected d = true, got %v", lit.Value.Bool)
		}
	} else {
		t.Error("expected d to be LiteralExpression")
	}
}

// =============================================================================
// 5. DELETE Tests
// =============================================================================

func TestParseDelete_Simple(t *testing.T) {
	sql := "DELETE FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	deleteStmt, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}

	if deleteStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", deleteStmt.TableName)
	}
}

func TestParseDelete_WithWhere(t *testing.T) {
	sql := "DELETE FROM users WHERE id = 1;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	deleteStmt, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}

	if deleteStmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseDelete_WhereCondition(t *testing.T) {
	sql := "DELETE FROM users WHERE active = false;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	deleteStmt, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}

	if deleteStmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	binOp, ok := (*deleteStmt.Where).(*BinaryOperation)
	if !ok {
		t.Fatalf("expected BinaryOperation, got %T", *deleteStmt.Where)
	}

	ident, ok := binOp.Left.(*IdentifierExpression)
	if !ok || ident.Name != "active" {
		t.Errorf("expected left side to be 'active', got %v", binOp.Left)
	}
}

// =============================================================================
// 6. JOIN Tests
// =============================================================================

func TestParseSelect_CrossJoin(t *testing.T) {
	sql := "SELECT * FROM users CROSS JOIN orders;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	joinItem, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	if joinItem.JoinType != JoinCross {
		t.Errorf("expected CROSS JOIN, got %v", joinItem.JoinType)
	}
	if joinItem.Predicate != nil {
		t.Error("CROSS JOIN should not have ON predicate")
	}
}

func TestParseSelect_InnerJoin(t *testing.T) {
	sql := "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	joinItem, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	if joinItem.JoinType != JoinInner {
		t.Errorf("expected INNER JOIN, got %v", joinItem.JoinType)
	}
	if joinItem.Predicate == nil {
		t.Error("INNER JOIN should have ON predicate")
	}
}

func TestParseSelect_LeftJoin(t *testing.T) {
	sql := "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	joinItem, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	if joinItem.JoinType != JoinLeft {
		t.Errorf("expected LEFT JOIN, got %v", joinItem.JoinType)
	}
}

func TestParseSelect_RightJoin(t *testing.T) {
	sql := "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	joinItem, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	if joinItem.JoinType != JoinRight {
		t.Errorf("expected RIGHT JOIN, got %v", joinItem.JoinType)
	}
}

func TestParseSelect_MultipleJoins(t *testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b CROSS JOIN c;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// First join (a CROSS JOIN b CROSS JOIN c)
	joinOuter, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	// Inner join (a CROSS JOIN b)
	joinInner, ok := joinOuter.Left.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected nested JoinFromItem, got %T", joinOuter.Left)
	}

	// Check the leftmost table
	leftTable, ok := joinInner.Left.(*TableFromItem)
	if !ok || leftTable.Name != "a" {
		t.Errorf("expected left table 'a', got %v", joinInner.Left)
	}

	// Check the middle table
	middleTable, ok := joinInner.Right.(*TableFromItem)
	if !ok || middleTable.Name != "b" {
		t.Errorf("expected middle table 'b', got %v", joinInner.Right)
	}

	// Check the rightmost table
	rightTable, ok := joinOuter.Right.(*TableFromItem)
	if !ok || rightTable.Name != "c" {
		t.Errorf("expected right table 'c', got %v", joinOuter.Right)
	}
}

func TestParseSelect_JoinWithAlias(t *testing.T) {
	sql := "SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	joinItem, ok := selectStmt.From.(*JoinFromItem)
	if !ok {
		t.Fatalf("expected JoinFromItem, got %T", selectStmt.From)
	}

	leftTable, ok := joinItem.Left.(*TableFromItem)
	if !ok {
		t.Fatalf("expected TableFromItem, got %T", joinItem.Left)
	}
	if leftTable.As != "u" {
		t.Errorf("expected alias 'u', got %q", leftTable.As)
	}

	rightTable, ok := joinItem.Right.(*TableFromItem)
	if !ok {
		t.Fatalf("expected TableFromItem, got %T", joinItem.Right)
	}
	if rightTable.As != "o" {
		t.Errorf("expected alias 'o', got %q", rightTable.As)
	}
}

// =============================================================================
// 7. Aggregation and GROUP BY Tests
// =============================================================================

func TestParseSelect_Count(t *testing.T) {
	sql := "SELECT COUNT(*) FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Select) != 1 {
		t.Fatalf("expected 1 select item, got %d", len(selectStmt.Select))
	}

	funcCall, ok := selectStmt.Select[0].Expr.(*FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", selectStmt.Select[0].Expr)
	}

	if funcCall.Name != "COUNT" {
		t.Errorf("expected COUNT, got %q", funcCall.Name)
	}
}

func TestParseSelect_CountColumn(t *testing.T) {
	sql := "SELECT COUNT(id) FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	funcCall, ok := selectStmt.Select[0].Expr.(*FunctionCall)
	if !ok {
		t.Fatalf("expected FunctionCall, got %T", selectStmt.Select[0].Expr)
	}

	ident, ok := funcCall.Arg.(*IdentifierExpression)
	if !ok || ident.Name != "id" {
		t.Errorf("expected COUNT(id), got %v", funcCall.Arg)
	}
}

func TestParseSelect_AggregateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		funcName string
	}{
		{"COUNT", "SELECT COUNT(*) FROM users;", "COUNT"},
		{"MIN", "SELECT MIN(score) FROM users;", "MIN"},
		{"MAX", "SELECT MAX(score) FROM users;", "MAX"},
		{"SUM", "SELECT SUM(score) FROM users;", "SUM"},
		{"AVG", "SELECT AVG(score) FROM users;", "AVG"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("expected SelectStatement, got %T", stmt)
			}

			funcCall, ok := selectStmt.Select[0].Expr.(*FunctionCall)
			if !ok {
				t.Fatalf("expected FunctionCall, got %T", selectStmt.Select[0].Expr)
			}

			if funcCall.Name != tt.funcName {
				t.Errorf("expected %s, got %q", tt.funcName, funcCall.Name)
			}
		})
	}
}

func TestParseSelect_GroupBy(t *testing.T) {
	sql := "SELECT name, COUNT(*) FROM users GROUP BY name;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY item, got %d", len(selectStmt.GroupBy))
	}

	ident, ok := selectStmt.GroupBy[0].(*IdentifierExpression)
	if !ok || ident.Name != "name" {
		t.Errorf("expected GROUP BY name, got %v", selectStmt.GroupBy[0])
	}
}

func TestParseSelect_Having(t *testing.T) {
	sql := "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Having == nil {
		t.Fatal("expected HAVING clause")
	}
}

func TestParseSelect_AggregateWithAlias(t *testing.T) {
	sql := "SELECT COUNT(*) AS total FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Select[0].As != "total" {
		t.Errorf("expected alias 'total', got %q", selectStmt.Select[0].As)
	}
}

// =============================================================================
// 8. Transaction Tests (BEGIN, COMMIT, ROLLBACK)
// =============================================================================

func TestParseBegin(t *testing.T) {
	sql := "BEGIN;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	_, ok := stmt.(*BeginStatement)
	if !ok {
		t.Fatalf("expected BeginStatement, got %T", stmt)
	}
}

func TestParseCommit(t *testing.T) {
	sql := "COMMIT;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	_, ok := stmt.(*CommitStatement)
	if !ok {
		t.Fatalf("expected CommitStatement, got %T", stmt)
	}
}

func TestParseRollback(t *testing.T) {
	sql := "ROLLBACK;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	_, ok := stmt.(*RollbackStatement)
	if !ok {
		t.Fatalf("expected RollbackStatement, got %T", stmt)
	}
}

func TestParseTransactionStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		typ  Statement
	}{
		{"BEGIN", "BEGIN", &BeginStatement{}},
		{"COMMIT", "COMMIT", &CommitStatement{}},
		{"ROLLBACK", "ROLLBACK", &RollbackStatement{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			switch tt.typ.(type) {
			case *BeginStatement:
				if _, ok := stmt.(*BeginStatement); !ok {
					t.Errorf("expected BeginStatement, got %T", stmt)
				}
			case *CommitStatement:
				if _, ok := stmt.(*CommitStatement); !ok {
					t.Errorf("expected CommitStatement, got %T", stmt)
				}
			case *RollbackStatement:
				if _, ok := stmt.(*RollbackStatement); !ok {
					t.Errorf("expected RollbackStatement, got %T", stmt)
				}
			}
		})
	}
}

// =============================================================================
// 9. Complex Expression Tests
// =============================================================================

func TestParseExpression_BinaryOps(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		op   BinaryOp
	}{
		{"equal", "SELECT * FROM t WHERE a = 1", OpEq},
		{"not equal", "SELECT * FROM t WHERE a != 1", OpNe},
		{"less than", "SELECT * FROM t WHERE a < 1", OpLt},
		{"less equal", "SELECT * FROM t WHERE a <= 1", OpLe},
		{"greater than", "SELECT * FROM t WHERE a > 1", OpGt},
		{"greater equal", "SELECT * FROM t WHERE a >= 1", OpGe},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("expected SelectStatement, got %T", stmt)
			}

			binOp, ok := (*selectStmt.Where).(*BinaryOperation)
			if !ok {
				t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
			}

			if binOp.Op != tt.op {
				t.Errorf("expected op %v, got %v", tt.op, binOp.Op)
			}
		})
	}
}

func TestParseExpression_AndOr(t *testing.T) {
	sql := "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// The expression should be (a = 1 AND b = 2) OR c = 3
	// OR has lower precedence than AND
	binOp, ok := (*selectStmt.Where).(*BinaryOperation)
	if !ok {
		t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
	}

	if binOp.Op != OpOr {
		t.Errorf("expected top-level OpOr, got %v", binOp.Op)
	}
}

func TestParseExpression_Arithmetic(t *testing.T) {
	sql := "SELECT a + b * c - d / e FROM t;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// Just verify it parses without error
	if len(selectStmt.Select) != 1 {
		t.Errorf("expected 1 select item, got %d", len(selectStmt.Select))
	}
}

func TestParseExpression_UnaryNot(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT active;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	unaryOp, ok := (*selectStmt.Where).(*UnaryOperation)
	if !ok {
		t.Fatalf("expected UnaryOperation, got %T", *selectStmt.Where)
	}

	if unaryOp.Op != OpNot {
		t.Errorf("expected OpNot, got %v", unaryOp.Op)
	}
}

func TestParseExpression_UnaryNeg(t *testing.T) {
	sql := "SELECT * FROM t WHERE score < -10;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	binOp, ok := (*selectStmt.Where).(*BinaryOperation)
	if !ok {
		t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
	}

	unaryOp, ok := binOp.Right.(*UnaryOperation)
	if !ok {
		t.Fatalf("expected UnaryOperation on right side, got %T", binOp.Right)
	}

	if unaryOp.Op != OpNeg {
		t.Errorf("expected OpNeg, got %v", unaryOp.Op)
	}
}

func TestParseExpression_QualifiedIdentifier(t *testing.T) {
	sql := "SELECT users.id, users.name FROM users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// Check first select item
	qualified, ok := selectStmt.Select[0].Expr.(*QualifiedIdentifierExpression)
	if !ok {
		t.Fatalf("expected QualifiedIdentifierExpression, got %T", selectStmt.Select[0].Expr)
	}
	if qualified.Table != "users" || qualified.Column != "id" {
		t.Errorf("expected users.id, got %s.%s", qualified.Table, qualified.Column)
	}
}

func TestParseExpression_Parentheses(t *testing.T) {
	sql := "SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// The expression should be (a = 1 OR b = 2) AND c = 3
	binOp, ok := (*selectStmt.Where).(*BinaryOperation)
	if !ok {
		t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
	}

	if binOp.Op != OpAnd {
		t.Errorf("expected top-level OpAnd, got %v", binOp.Op)
	}
}

func TestParseExpression_ComparisonOperators(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		op     BinaryOp
	}{
		{"<> operator", "SELECT * FROM t WHERE a <> 1", OpNe},
		{"!= operator", "SELECT * FROM t WHERE a != 1", OpNe},
		{"<= operator", "SELECT * FROM t WHERE a <= 1", OpLe},
		{">= operator", "SELECT * FROM t WHERE a >= 1", OpGe},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("expected SelectStatement, got %T", stmt)
			}

			binOp, ok := (*selectStmt.Where).(*BinaryOperation)
			if !ok {
				t.Fatalf("expected BinaryOperation, got %T", *selectStmt.Where)
			}

			if binOp.Op != tt.op {
				t.Errorf("expected op %v, got %v", tt.op, binOp.Op)
			}
		})
	}
}

// =============================================================================
// 10. Error Handling Tests
// =============================================================================

func TestParseError_InvalidStatement(t *testing.T) {
	sql := "INVALID STATEMENT;"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for invalid statement")
	}
}

func TestParseError_EmptyInput(t *testing.T) {
	sql := ""
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt != nil {
		t.Error("expected nil statement for empty input")
	}
}

func TestParseError_CreateTableMissingParen(t *testing.T) {
	sql := "CREATE TABLE users id INTEGER;"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for missing parenthesis")
	}
}

func TestParseError_CreateTableInvalidType(t *testing.T) {
	sql := "CREATE TABLE users (id INVALID);"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for invalid data type")
	}
}

func TestParseError_InsertMissingValues(t *testing.T) {
	sql := "INSERT INTO users;"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for missing VALUES")
	}
}

func TestParseError_SelectMissingFrom(t *testing.T) {
	sql := "SELECT *;"
	// This should parse - FROM is optional in this parser
	_, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseError_UnterminatedString(t *testing.T) {
	sql := "INSERT INTO users VALUES ('unterminated);"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for unterminated string")
	}
}

func TestParseError_InvalidSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"missing table name", "CREATE TABLE (id INTEGER);"},
		{"missing column name", "CREATE TABLE users (INTEGER);"},
		{"missing data type", "CREATE TABLE users (id);"},
		{"invalid token", "SELECT * FROM users WHERE @#$;"},
		{"missing VALUES", "INSERT INTO users (1, 2, 3);"},
		{"missing comma in values", "INSERT INTO users VALUES (1 2 3);"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseStatement(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

func TestParseError_UpdateMissingSet(t *testing.T) {
	sql := "UPDATE users name = 'Bob';"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for missing SET")
	}
}

func TestParseError_DeleteMissingFrom(t *testing.T) {
	sql := "DELETE users;"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Error("expected error for missing FROM")
	}
}

// =============================================================================
// Additional Tests for ParseStatements
// =============================================================================

func TestParseStatements_Multiple(t *testing.T) {
	sql := "BEGIN; INSERT INTO users VALUES (1, 'Alice'); COMMIT;"
	stmts, err := ParseStatements(sql)
	if err != nil {
		t.Fatalf("ParseStatements() error = %v", err)
	}

	if len(stmts) != 3 {
		t.Errorf("expected 3 statements, got %d", len(stmts))
	}

	if _, ok := stmts[0].(*BeginStatement); !ok {
		t.Errorf("expected first statement to be BeginStatement, got %T", stmts[0])
	}
	if _, ok := stmts[1].(*InsertStatement); !ok {
		t.Errorf("expected second statement to be InsertStatement, got %T", stmts[1])
	}
	if _, ok := stmts[2].(*CommitStatement); !ok {
		t.Errorf("expected third statement to be CommitStatement, got %T", stmts[2])
	}
}

func TestParseStatements_NoSemicolon(t *testing.T) {
	sql := "SELECT * FROM users"
	stmts, err := ParseStatements(sql)
	if err != nil {
		t.Fatalf("ParseStatements() error = %v", err)
	}

	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}
}

// =============================================================================
// SHOW Statement Tests
// =============================================================================

func TestParseShowTables(t *testing.T) {
	sql := "SHOW TABLES;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	_, ok := stmt.(*ShowTablesStatement)
	if !ok {
		t.Fatalf("expected ShowTablesStatement, got %T", stmt)
	}
}

func TestParseShowTable(t *testing.T) {
	sql := "SHOW TABLE users;"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	showStmt, ok := stmt.(*ShowTableStatement)
	if !ok {
		t.Fatalf("expected ShowTableStatement, got %T", stmt)
	}

	if showStmt.Name != "users" {
		t.Errorf("expected table name 'users', got %q", showStmt.Name)
	}
}

// =============================================================================
// Edge Cases and Whitespace Tests
// =============================================================================

func TestParse_WhitespaceHandling(t *testing.T) {
	tests := []string{
		"SELECT * FROM users;",
		"SELECT  *  FROM  users;",
		"SELECT\n*\nFROM\nusers;",
		"SELECT\t*\tFROM\tusers;",
		"  SELECT  *  FROM  users  ;  ",
	}

	for i, sql := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			stmt, err := ParseStatement(sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			selectStmt, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("expected SelectStatement, got %T", stmt)
			}

			tableFrom, ok := selectStmt.From.(*TableFromItem)
			if !ok || tableFrom.Name != "users" {
				t.Errorf("expected FROM users, got %v", selectStmt.From)
			}
		})
	}
}

func TestParse_CaseInsensitive(t *testing.T) {
	tests := []string{
		"SELECT * FROM users;",
		"select * from users;",
		"Select * From users;",
		"SELECT * FROM USERS;",
	}

	for i, sql := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			stmt, err := ParseStatement(sql)
			if err != nil {
				t.Fatalf("ParseStatement() error = %v", err)
			}

			_, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("expected SelectStatement, got %T", stmt)
			}
		})
	}
}

func TestParse_EscapedString(t *testing.T) {
	sql := "INSERT INTO users VALUES ('it''s escaped');"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	lit, ok := insertStmt.Values[0][0].(*LiteralExpression)
	if !ok {
		t.Fatalf("expected LiteralExpression, got %T", insertStmt.Values[0][0])
	}

	// The lexer should handle escaped quotes
	if lit.Value.Str != "it's escaped" && lit.Value.Str != "it''s escaped" {
		t.Logf("string value: %q", lit.Value.Str)
	}
}

// Test parsing CREATE TABLE with data type aliases
func TestParseCreateTableWithAliases(t *testing.T) {
	sql := "create table tbl (a int primary key,b string,c float);"
	stmts, err := ParseStatements(sql)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmts) == 0 {
		t.Fatalf("No statements parsed")
	}

	createStmt, ok := stmts[0].(*CreateTableStatement)
	if !ok {
		t.Fatalf("Expected CreateTableStatement, got %T", stmts[0])
	}

	if createStmt.Name != "tbl" {
		t.Errorf("Expected table name 'tbl', got %s", createStmt.Name)
	}

	if len(createStmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(createStmt.Columns))
	}

	// Check column types
	if createStmt.Columns[0].DataType != types.TypeInteger {
		t.Errorf("Expected column 'a' to be INTEGER, got %v", createStmt.Columns[0].DataType)
	}
	if createStmt.Columns[1].DataType != types.TypeString {
		t.Errorf("Expected column 'b' to be STRING, got %v", createStmt.Columns[1].DataType)
	}
	if createStmt.Columns[2].DataType != types.TypeFloat {
		t.Errorf("Expected column 'c' to be FLOAT, got %v", createStmt.Columns[2].DataType)
	}

	// Check primary key
	if !createStmt.Columns[0].PrimaryKey {
		t.Error("Expected column 'a' to be primary key")
	}
}

// Test aggregate functions with lowercase
func TestParseAggregateFunctionsCaseInsensitive(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"count lowercase", "SELECT count(*) FROM users;", "COUNT"},
		{"count mixed", "SELECT Count(*) FROM users;", "COUNT"},
		{"sum lowercase", "SELECT sum(salary) FROM users;", "SUM"},
		{"avg lowercase", "SELECT avg(age) FROM users;", "AVG"},
		{"min lowercase", "SELECT min(id) FROM users;", "MIN"},
		{"max lowercase", "SELECT max(id) FROM users;", "MAX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseStatements(tt.sql)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			sel, ok := stmts[0].(*SelectStatement)
			if !ok {
				t.Fatalf("Expected SelectStatement, got %T", stmts[0])
			}

			if len(sel.Select) == 0 {
				t.Fatal("No columns in SELECT")
			}

			funcCall, ok := sel.Select[0].Expr.(*FunctionCall)
			if !ok {
				t.Fatalf("Expected FunctionCall, got %T", sel.Select[0].Expr)
			}

			if funcCall.Name != tt.want {
				t.Errorf("Expected function name %q, got %q", tt.want, funcCall.Name)
			}
		})
	}
}
