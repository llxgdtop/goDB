package engine

import (
	"os"
	"testing"

	"github.com/llxgdtop/godb/internal/sql/executor"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
	"github.com/llxgdtop/godb/internal/storage"
)

type testEngine struct {
	*KVEngine
	cleanupDir string
}

func setupTestEngine(t *testing.T) *testEngine {
	dir, err := os.MkdirTemp("", "godb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	diskEngine, err := storage.NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}

	return &testEngine{
		KVEngine:   NewKVEngine(diskEngine),
		cleanupDir: dir,
	}
}

func (te *testEngine) Cleanup() {
	if te.cleanupDir != "" {
		os.RemoveAll(te.cleanupDir)
	}
}

func executeSQL(eng *testEngine, sql string) (*executor.Result, error) {
	stmts, err := parser.ParseStatements(sql)
	if err != nil {
		return nil, err
	}

	planner := plan.NewPlanner(eng.GetTables())
	node, err := planner.Plan(stmts[0])
	if err != nil {
		return nil, err
	}

	exec := executor.NewExecutor(eng)
	return exec.Execute(node)
}

func TestCreateTable(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create simple table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	table, ok := eng.GetTable("test")
	if !ok {
		t.Fatal("Table not found")
	}
	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
}

func TestInsert(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with all columns
	_, err = executeSQL(eng, "INSERT INTO test (id, name, age) VALUES (1, 'Alice', 30);")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Insert with column reordering
	_, err = executeSQL(eng, "INSERT INTO test (name, id, age) VALUES ('Bob', 2, 25);")
	if err != nil {
		t.Fatalf("Failed to insert with reordered columns: %v", err)
	}

	// Verify data
	result, err := executeSQL(eng, "SELECT * FROM test;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
}

func TestUpdate(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 'Alice', 100);")
	executeSQL(eng, "INSERT INTO test VALUES (2, 'Bob', 200);")
	executeSQL(eng, "INSERT INTO test VALUES (3, 'Charlie', 300);")

	// Update single row
	result, err := executeSQL(eng, "UPDATE test SET value = 150 WHERE id = 1;")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	if result.Message != "UPDATE 1 rows" {
		t.Errorf("Expected 'UPDATE 1 rows', got '%s'", result.Message)
	}

	// Update multiple rows (value > 100 matches all 3 rows: 150, 200, 300)
	result, err = executeSQL(eng, "UPDATE test SET name = 'Updated' WHERE value > 100;")
	if err != nil {
		t.Fatalf("Failed to update multiple: %v", err)
	}
	if result.Message != "UPDATE 3 rows" {
		t.Errorf("Expected 'UPDATE 3 rows', got '%s'", result.Message)
	}
}

func TestDelete(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 'Alice');")
	executeSQL(eng, "INSERT INTO test VALUES (2, 'Bob');")
	executeSQL(eng, "INSERT INTO test VALUES (3, 'Charlie');")
	executeSQL(eng, "INSERT INTO test VALUES (4, 'David');")

	// Delete single row
	result, err := executeSQL(eng, "DELETE FROM test WHERE id = 1;")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if result.Message != "DELETE 1 rows" {
		t.Errorf("Expected 'DELETE 1 rows', got '%s'", result.Message)
	}

	// Delete multiple rows
	result, err = executeSQL(eng, "DELETE FROM test WHERE id > 2;")
	if err != nil {
		t.Fatalf("Failed to delete multiple: %v", err)
	}
	if result.Message != "DELETE 2 rows" {
		t.Errorf("Expected 'DELETE 2 rows', got '%s'", result.Message)
	}

	// Verify remaining data
	result, err = executeSQL(eng, "SELECT * FROM test;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestSort(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER, name STRING)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 30, 'C');")
	executeSQL(eng, "INSERT INTO test VALUES (2, 10, 'A');")
	executeSQL(eng, "INSERT INTO test VALUES (3, 20, 'B');")

	// Test ORDER BY ASC
	result, err := executeSQL(eng, "SELECT * FROM test ORDER BY value;")
	if err != nil {
		t.Fatalf("Failed to select with order: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
	// First row should have value 10
	if result.Rows[0][1].Int != 10 {
		t.Errorf("Expected first row value 10, got %d", result.Rows[0][1].Int)
	}

	// Test ORDER BY DESC
	result, err = executeSQL(eng, "SELECT * FROM test ORDER BY value DESC;")
	if err != nil {
		t.Fatalf("Failed to select with order desc: %v", err)
	}
	// First row should have value 30
	if result.Rows[0][1].Int != 30 {
		t.Errorf("Expected first row value 30, got %d", result.Rows[0][1].Int)
	}
}

func TestCrossJoin(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create tables
	executeSQL(eng, "CREATE TABLE t1 (a INTEGER PRIMARY KEY);")
	executeSQL(eng, "CREATE TABLE t2 (b INTEGER PRIMARY KEY);")

	// Populate tables
	executeSQL(eng, "INSERT INTO t1 VALUES (1);")
	executeSQL(eng, "INSERT INTO t1 VALUES (2);")
	executeSQL(eng, "INSERT INTO t1 VALUES (3);")
	executeSQL(eng, "INSERT INTO t2 VALUES (4);")
	executeSQL(eng, "INSERT INTO t2 VALUES (5);")
	executeSQL(eng, "INSERT INTO t2 VALUES (6);")

	// Cross join should produce 9 rows (3 * 3)
	result, err := executeSQL(eng, "SELECT * FROM t1 CROSS JOIN t2;")
	if err != nil {
		t.Fatalf("Failed to cross join: %v", err)
	}
	if len(result.Rows) != 9 {
		t.Errorf("Expected 9 rows from cross join, got %d", len(result.Rows))
	}
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
}

func TestJoin(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create tables
	executeSQL(eng, "CREATE TABLE t1 (a INTEGER PRIMARY KEY);")
	executeSQL(eng, "CREATE TABLE t2 (b INTEGER PRIMARY KEY);")

	// Populate tables with some overlapping values
	executeSQL(eng, "INSERT INTO t1 VALUES (1);")
	executeSQL(eng, "INSERT INTO t1 VALUES (2);")
	executeSQL(eng, "INSERT INTO t1 VALUES (3);")
	executeSQL(eng, "INSERT INTO t2 VALUES (2);")
	executeSQL(eng, "INSERT INTO t2 VALUES (3);")
	executeSQL(eng, "INSERT INTO t2 VALUES (4);")

	// Inner join should produce 2 rows (2 and 3 match)
	result, err := executeSQL(eng, "SELECT * FROM t1 JOIN t2 ON a = b;")
	if err != nil {
		t.Fatalf("Failed to join: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows from join, got %d", len(result.Rows))
	}
}

func TestAggregate(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 10);")
	executeSQL(eng, "INSERT INTO test VALUES (2, 20);")
	executeSQL(eng, "INSERT INTO test VALUES (3, 30);")
	executeSQL(eng, "INSERT INTO test VALUES (4, 40);")

	// Test COUNT
	result, err := executeSQL(eng, "SELECT COUNT(*) FROM test;")
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// Test SUM (returns float)
	result, err = executeSQL(eng, "SELECT SUM(value) FROM test;")
	if err != nil {
		t.Fatalf("Failed to sum: %v", err)
	}
	if result.Rows[0][0].Float != 100.0 {
		t.Errorf("Expected sum 100, got %f", result.Rows[0][0].Float)
	}

	// Test AVG
	result, err = executeSQL(eng, "SELECT AVG(value) FROM test;")
	if err != nil {
		t.Fatalf("Failed to avg: %v", err)
	}
	// AVG might be float
	avg := result.Rows[0][0].Float
	if avg < 24.9 || avg > 25.1 {
		t.Errorf("Expected avg ~25, got %f", avg)
	}

	// Test MIN
	result, err = executeSQL(eng, "SELECT MIN(value) FROM test;")
	if err != nil {
		t.Fatalf("Failed to min: %v", err)
	}
	t.Logf("MIN: Columns=%v, Rows=%v", result.Columns, result.Rows)
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatalf("MIN: No rows returned")
	}
	if result.Rows[0][0].Int != 10 {
		t.Errorf("Expected min 10, got %d", result.Rows[0][0].Int)
	}

	// Test MAX
	result, err = executeSQL(eng, "SELECT MAX(value) FROM test;")
	if err != nil {
		t.Fatalf("Failed to max: %v", err)
	}
	t.Logf("MAX: Columns=%v, Rows=%v", result.Columns, result.Rows)
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatalf("MAX: No rows returned")
	}
	if result.Rows[0][0].Int != 40 {
		t.Errorf("Expected max 40, got %d", result.Rows[0][0].Int)
	}
}

func TestGroupBy(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, category STRING, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 'A', 10);")
	executeSQL(eng, "INSERT INTO test VALUES (2, 'A', 20);")
	executeSQL(eng, "INSERT INTO test VALUES (3, 'B', 30);")
	executeSQL(eng, "INSERT INTO test VALUES (4, 'B', 40);")
	executeSQL(eng, "INSERT INTO test VALUES (5, 'C', 50);")

	// Test GROUP BY
	result, err := executeSQL(eng, "SELECT category, SUM(value) FROM test GROUP BY category;")
	if err != nil {
		t.Fatalf("Failed to group by: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 groups, got %d rows", len(result.Rows))
	}
}

func TestFilter(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create and populate table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING, active BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	executeSQL(eng, "INSERT INTO test VALUES (1, 'Alice', TRUE);")
	executeSQL(eng, "INSERT INTO test VALUES (2, 'Bob', FALSE);")
	executeSQL(eng, "INSERT INTO test VALUES (3, 'Charlie', TRUE);")
	executeSQL(eng, "INSERT INTO test VALUES (4, 'David', FALSE);")

	// Test WHERE with comparison
	result, err := executeSQL(eng, "SELECT * FROM test WHERE id > 2;")
	if err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Test WHERE with boolean
	result, err = executeSQL(eng, "SELECT * FROM test WHERE active = TRUE;")
	if err != nil {
		t.Fatalf("Failed to filter by boolean: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with active=TRUE, got %d", len(result.Rows))
	}

	// Test WHERE with AND
	result, err = executeSQL(eng, "SELECT * FROM test WHERE id > 1 AND active = FALSE;")
	if err != nil {
		t.Fatalf("Failed to filter with AND: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with AND condition, got %d", len(result.Rows))
	}
}

func TestTransactionBeginCommit(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	result, err := executeSQL(eng, "BEGIN;")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	// Check that message contains "BEGIN" (version number may vary)
	if result.Message == "BEGIN" || result.Message == "" {
		t.Errorf("Expected transaction begin message with version, got '%s'", result.Message)
	}
	if len(result.Message) < 10 || result.Message[len(result.Message)-5:] != "BEGIN" {
		t.Errorf("Expected message ending with 'BEGIN', got '%s'", result.Message)
	}

	// Insert in transaction
	_, err = executeSQL(eng, "INSERT INTO test VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Commit
	result, err = executeSQL(eng, "COMMIT;")
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	// Check that message contains "COMMIT" (version number may vary)
	if len(result.Message) < 10 || result.Message[len(result.Message)-6:] != "COMMIT" {
		t.Errorf("Expected message ending with 'COMMIT', got '%s'", result.Message)
	}

	// Verify data persists
	result, err = executeSQL(eng, "SELECT * FROM test;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row after commit, got %d", len(result.Rows))
	}
}

func TestTransactionRollback(t *testing.T) {
	eng := setupTestEngine(t)
	defer eng.Cleanup()

	// Create table
	_, err := executeSQL(eng, "CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	result, err := executeSQL(eng, "BEGIN;")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	// Check that message contains "BEGIN"
	if len(result.Message) < 10 || result.Message[len(result.Message)-5:] != "BEGIN" {
		t.Errorf("Expected message ending with 'BEGIN', got '%s'", result.Message)
	}

	// Insert in transaction
	_, err = executeSQL(eng, "INSERT INTO test VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Rollback
	result, err = executeSQL(eng, "ROLLBACK;")
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}
	// Check that message contains "ROLLBACK" (version number may vary)
	if len(result.Message) < 12 || result.Message[len(result.Message)-8:] != "ROLLBACK" {
		t.Errorf("Expected message ending with 'ROLLBACK', got '%s'", result.Message)
	}

	// Verify data was rolled back
	result, err = executeSQL(eng, "SELECT * FROM test;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", len(result.Rows))
	}
}

// Test result message formats match RustDB
func TestResultMessageFormats(t *testing.T) {
	eng := &testEngine{KVEngine: NewMemoryKVEngine()}

	// Test CREATE TABLE format
	result, err := executeSQL(eng, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	expected := "CREATE TABLE users"
	if result.Message != expected {
		t.Errorf("CREATE TABLE: expected '%s', got '%s'", expected, result.Message)
	}

	// Test INSERT format
	result, err = executeSQL(eng, "INSERT INTO users (id) VALUES (1);")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	expected = "INSERT 1 rows"
	if result.Message != expected {
		t.Errorf("INSERT: expected '%s', got '%s'", expected, result.Message)
	}

	// Test SELECT format (should have parentheses around row count)
	result, err = executeSQL(eng, "SELECT * FROM users;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	// Check the formatted output
	formatted := executor.FormatResult(result)
	if formatted == "" {
		t.Error("SELECT: expected non-empty formatted result")
	}

	// Test UPDATE format
	result, err = executeSQL(eng, "UPDATE users SET id = 2 WHERE id = 1;")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	expected = "UPDATE 1 rows"
	if result.Message != expected {
		t.Errorf("UPDATE: expected '%s', got '%s'", expected, result.Message)
	}

	// Test DELETE format
	result, err = executeSQL(eng, "DELETE FROM users WHERE id = 2;")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	expected = "DELETE 1 rows"
	if result.Message != expected {
		t.Errorf("DELETE: expected '%s', got '%s'", expected, result.Message)
	}

	// Test BEGIN format
	result, err = executeSQL(eng, "BEGIN;")
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}
	// Should contain transaction number
	if result.Message == "BEGIN" {
		t.Errorf("BEGIN: expected 'TRANSACTION N BEGIN' format, got '%s'", result.Message)
	}

	// Test COMMIT format
	result, err = executeSQL(eng, "COMMIT;")
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	if result.Message == "COMMIT" {
		t.Errorf("COMMIT: expected 'TRANSACTION N COMMIT' format, got '%s'", result.Message)
	}
}

// Helper function to compare rows
func rowsEqual(a, b types.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Compare(b[i]) != 0 {
			return false
		}
	}
	return true
}
