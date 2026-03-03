package executor

import (
	"os"
	"testing"

	"github.com/llxgdtop/godb/internal/sql/engine"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/storage"
)

// TestUserReportedBug_MultiRowInsertAtomicity tests the exact scenario the user reported:
// insert into tbl values (1, 1.1), (1,2.2); should fail AND not insert any rows
func TestUserReportedBug_MultiRowInsertAtomicity(t *testing.T) {
	dir, err := os.MkdirTemp("", "godb-integration-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	diskEngine, err := storage.NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}

	eng := engine.NewKVEngine(diskEngine)

	// Helper function to execute SQL
	executeSQL := func(sql string) (*Result, error) {
		stmts, err := parser.ParseStatements(sql)
		if err != nil {
			return nil, err
		}
		planner := plan.NewPlanner(eng.GetTables())
		node, err := planner.Plan(stmts[0])
		if err != nil {
			return nil, err
		}
		exec := NewExecutor(eng)
		return exec.Execute(node)
	}

	// Step 1: Create table
	t.Log("Step 1: CREATE TABLE tbl (a int primary key, b float);")
	_, err = executeSQL("CREATE TABLE tbl (a int primary key, b float);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Step 2: Insert with duplicate key (user's exact scenario)
	t.Log("Step 2: INSERT INTO tbl VALUES (1, 1.1), (1, 2.2);")
	_, err = executeSQL("INSERT INTO tbl VALUES (1, 1.1), (1, 2.2);")
	if err == nil {
		t.Fatal("Expected duplicate key error, but got none")
	}
	t.Logf("Got expected error: %v", err)

	// Step 3: Verify NO rows were inserted (atomicity)
	t.Log("Step 3: SELECT * FROM tbl; (should be empty)")
	result, err := executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after failed insert, got %d rows: %v", len(result.Rows), result.Rows)
		t.Fatal("BUG: Multi-row INSERT is NOT atomic - some rows were inserted despite the error!")
	}
	t.Log("PASS: No rows were inserted - atomicity maintained!")

	// Step 4: Insert valid rows (key 1 is now available since previous failed insert was rolled back)
	t.Log("Step 4: INSERT INTO tbl VALUES (1, 1.1), (2, 2.2);")
	result, err = executeSQL("INSERT INTO tbl VALUES (1, 1.1), (2, 2.2);")
	if err != nil {
		t.Fatalf("Failed to insert valid rows: %v", err)
	}
	if result.Message != "INSERT 2 rows" {
		t.Errorf("Expected 'INSERT 2 rows', got '%s'", result.Message)
	}

	// Step 5: Verify 2 rows were inserted
	t.Log("Step 5: SELECT * FROM tbl; (should have 2 rows)")
	result, err = executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d rows: %v", len(result.Rows), result.Rows)
	}

	// Step 6: Try to insert duplicates - should fail and not affect existing data
	t.Log("Step 6: INSERT INTO tbl VALUES (1, 5.5), (2, 6.6); (should fail due to duplicates)")
	_, err = executeSQL("INSERT INTO tbl VALUES (1, 5.5), (2, 6.6);")
	if err == nil {
		t.Fatal("Expected duplicate key error, but got none")
	}

	// Step 7: Verify original 2 rows still exist
	t.Log("Step 7: SELECT * FROM tbl; (should still have 2 rows)")
	result, err = executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d rows: %v", len(result.Rows), result.Rows)
	}

	t.Log("All tests passed! Multi-row INSERT atomicity is working correctly.")
}

// TestMultiRowInsertInTransaction tests that failed multi-row INSERTs within a transaction
// don't leave partial rows in the transaction buffer
func TestMultiRowInsertInTransaction(t *testing.T) {
	eng := engine.NewMemoryKVEngine()
	session := engine.NewSession(eng)

	// Helper function to execute SQL with session
	executeSQL := func(sql string) (*Result, error) {
		stmts, err := parser.ParseStatements(sql)
		if err != nil {
			return nil, err
		}
		planner := plan.NewPlanner(session.GetTables())
		node, err := planner.Plan(stmts[0])
		if err != nil {
			return nil, err
		}
		exec := NewSessionExecutor(session)
		return exec.Execute(node)
	}

	// Step 1: Create table and insert initial data
	t.Log("Step 1: Create table and insert initial row (2, 2.2)")
	_, err := executeSQL("CREATE TABLE tbl (a int primary key, b float);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = executeSQL("INSERT INTO tbl VALUES (2, 2.2);")
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Step 2: Begin transaction
	t.Log("Step 2: BEGIN TRANSACTION")
	_, err = executeSQL("BEGIN;")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Step 3: Try to insert (3,3.3) and (2,2.3) - should fail because key 2 exists
	t.Log("Step 3: INSERT INTO tbl VALUES (3, 3.3), (2, 2.3); (should fail, key 2 exists)")
	_, err = executeSQL("INSERT INTO tbl VALUES (3, 3.3), (2, 2.3);")
	if err == nil {
		t.Fatal("Expected duplicate key error, but got none")
	}
	t.Logf("Got expected error: %v", err)

	// Step 4: Try to insert (3,3.3) and (4,2.3) - should SUCCEED because key 3 was NOT inserted
	t.Log("Step 4: INSERT INTO tbl VALUES (3, 3.3), (4, 2.3); (should succeed)")
	result, err := executeSQL("INSERT INTO tbl VALUES (3, 3.3), (4, 2.3);")
	if err != nil {
		t.Fatalf("INSERT should have succeeded, but got error: %v", err)
	}
	if result.Message != "INSERT 2 rows" {
		t.Errorf("Expected 'INSERT 2 rows', got '%s'", result.Message)
	}

	// Step 5: Verify rows are visible within the transaction
	t.Log("Step 5: SELECT * FROM tbl; (should see 3 rows within transaction)")
	result, err = executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows within transaction, got %d: %v", len(result.Rows), result.Rows)
	}

	// Step 6: Commit transaction
	t.Log("Step 6: COMMIT")
	_, err = executeSQL("COMMIT;")
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Step 7: Verify final state
	t.Log("Step 7: SELECT * FROM tbl; (should see 3 rows after commit)")
	result, err = executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows after commit, got %d: %v", len(result.Rows), result.Rows)
	}

	t.Log("All tests passed! Multi-row INSERT in transaction atomicity is working correctly.")
}

// TestMultiRowInsertDuplicateWithinStatement tests that duplicate keys within
// the same INSERT statement are detected
func TestMultiRowInsertDuplicateWithinStatement(t *testing.T) {
	eng := engine.NewMemoryKVEngine()
	session := engine.NewSession(eng)

	executeSQL := func(sql string) (*Result, error) {
		stmts, err := parser.ParseStatements(sql)
		if err != nil {
			return nil, err
		}
		planner := plan.NewPlanner(session.GetTables())
		node, err := planner.Plan(stmts[0])
		if err != nil {
			return nil, err
		}
		exec := NewSessionExecutor(session)
		return exec.Execute(node)
	}

	// Create table
	_, err := executeSQL("CREATE TABLE tbl (a int primary key, b float);")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to insert with duplicate key within same statement
	t.Log("INSERT INTO tbl VALUES (1, 1.1), (1, 2.2); (duplicate key 1 in same statement)")
	_, err = executeSQL("INSERT INTO tbl VALUES (1, 1.1), (1, 2.2);")
	if err == nil {
		t.Fatal("Expected duplicate key error for same-statement duplicates")
	}
	t.Logf("Got expected error: %v", err)

	// Verify no rows were inserted
	result, err := executeSQL("SELECT * FROM tbl;")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result.Rows))
	}

	t.Log("PASS: Same-statement duplicate detection works correctly.")
}
