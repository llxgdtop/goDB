package engine

import (
	"testing"

	"github.com/llxgdtop/godb/internal/sql/types"
)

// TestCreateTableAndInsertInSameTransaction tests that tables created in a transaction
// are visible to subsequent operations in the same transaction
func TestCreateTableAndInsertInSameTransaction(t *testing.T) {
	engine := NewMemoryKVEngine()
	session := NewSession(engine)

	// Begin transaction
	if err := session.Begin(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table within the transaction
	table := &types.Table{
		Name: "test_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
			{Name: "value", DataType: types.TypeFloat},
		},
	}
	if err := session.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// The table should be visible to this session via GetTable
	foundTable, ok := session.GetTable("test_table")
	if !ok {
		t.Fatal("Table should be visible in same transaction via GetTable")
	}
	if foundTable.Name != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", foundTable.Name)
	}

	// The table should also be visible via GetTables
	tables := session.GetTables()
	if _, ok := tables["test_table"]; !ok {
		t.Fatal("Table should be visible in same transaction via GetTables")
	}

	// Insert should work because the table is visible
	if err := session.Insert("test_table", nil, []types.Value{
		{Type: types.TypeInteger, Int: 1},
		{Type: types.TypeFloat, Float: 2.2},
	}); err != nil {
		t.Fatalf("Failed to insert into table in same transaction: %v", err)
	}

	// Commit the transaction
	if err := session.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data was inserted
	rows, err := session.Scan("test_table", nil)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestCreateTableTransactionIsolation tests that tables created in uncommitted
// transactions are NOT visible to other sessions
func TestCreateTableTransactionIsolation(t *testing.T) {
	// Create an engine with memory storage
	engine := NewMemoryKVEngine()

	// Create two sessions (simulating two client connections)
	session1 := NewSession(engine)
	session2 := NewSession(engine)

	// Session 1: Create a table outside of transaction (should be visible to all)
	table1 := &types.Table{
		Name: "visible_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
			{Name: "name", DataType: types.TypeString},
		},
	}
	if err := session1.CreateTable(table1); err != nil {
		t.Fatalf("Failed to create visible_table: %v", err)
	}

	// Both sessions should see visible_table
	if tables := session1.GetTables(); len(tables) != 1 {
		t.Errorf("Session1 should see 1 table, got %d", len(tables))
	}
	if tables := session2.GetTables(); len(tables) != 1 {
		t.Errorf("Session2 should see 1 table, got %d", len(tables))
	}

	// Session 1: Begin transaction
	if err := session1.Begin(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Session 1: Create a table within the transaction
	table2 := &types.Table{
		Name: "pending_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
			{Name: "value", DataType: types.TypeFloat},
		},
	}
	if err := session1.CreateTable(table2); err != nil {
		t.Fatalf("Failed to create pending_table in transaction: %v", err)
	}

	// Session 1 should see BOTH tables (including the pending one)
	tables1 := session1.GetTables()
	if len(tables1) != 2 {
		t.Errorf("Session1 should see 2 tables (including pending), got %d", len(tables1))
	}
	if _, ok := tables1["pending_table"]; !ok {
		t.Error("Session1 should see pending_table")
	}

	// Session 2 should NOT see the pending table
	tables2 := session2.GetTables()
	if len(tables2) != 1 {
		t.Errorf("Session2 should still see only 1 table, got %d: %v", len(tables2), tables2)
	}
	if _, ok := tables2["pending_table"]; ok {
		t.Error("Session2 should NOT see pending_table before commit")
	}

	// Session 1: Commit the transaction
	if err := session1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now Session 2 should see both tables
	tables2 = session2.GetTables()
	if len(tables2) != 2 {
		t.Errorf("Session2 should see 2 tables after commit, got %d", len(tables2))
	}
	if _, ok := tables2["pending_table"]; !ok {
		t.Error("Session2 should see pending_table after commit")
	}
}

// TestCreateTableRollback tests that tables created in transactions are
// discarded on rollback
func TestCreateTableRollback(t *testing.T) {
	engine := NewMemoryKVEngine()
	session := NewSession(engine)

	// Create initial table
	table1 := &types.Table{
		Name: "initial_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
		},
	}
	if err := session.CreateTable(table1); err != nil {
		t.Fatalf("Failed to create initial_table: %v", err)
	}

	// Begin transaction
	if err := session.Begin(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table within the transaction
	table2 := &types.Table{
		Name: "temp_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
		},
	}
	if err := session.CreateTable(table2); err != nil {
		t.Fatalf("Failed to create temp_table: %v", err)
	}

	// Should see 2 tables within the session
	if tables := session.GetTables(); len(tables) != 2 {
		t.Errorf("Should see 2 tables before rollback, got %d", len(tables))
	}

	// Rollback the transaction
	if err := session.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Should only see the initial table after rollback
	if tables := session.GetTables(); len(tables) != 1 {
		t.Errorf("Should see 1 table after rollback, got %d: %v", len(tables), tables)
	}
	if _, ok := session.GetTable("temp_table"); ok {
		t.Error("temp_table should not exist after rollback")
	}
}

// TestCreateTableDuplicateInTransaction tests that creating a table with
// the same name in a transaction fails appropriately
func TestCreateTableDuplicateInTransaction(t *testing.T) {
	engine := NewMemoryKVEngine()
	session := NewSession(engine)

	// Create initial table
	table1 := &types.Table{
		Name: "test_table",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
		},
	}
	if err := session.CreateTable(table1); err != nil {
		t.Fatalf("Failed to create test_table: %v", err)
	}

	// Begin transaction and try to create same table
	if err := session.Begin(); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	table2 := &types.Table{
		Name: "test_table", // Same name
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
		},
	}
	if err := session.CreateTable(table2); err == nil {
		t.Error("Should fail when creating table with duplicate name")
	}

	// Rollback to clean up
	_ = session.Rollback()
}

// TestMultipleSessionsCreateTableIsolation tests that multiple sessions
// can create tables independently without seeing each other's uncommitted tables
func TestMultipleSessionsCreateTableIsolation(t *testing.T) {
	engine := NewMemoryKVEngine()

	session1 := NewSession(engine)
	session2 := NewSession(engine)
	session3 := NewSession(engine)

	// All sessions create tables in transactions
	_ = session1.Begin()
	_ = session2.Begin()
	_ = session3.Begin()

	table1 := &types.Table{
		Name: "session1_table",
		Columns: []types.Column{{Name: "id", DataType: types.TypeInteger, PrimaryKey: true}},
	}
	table2 := &types.Table{
		Name: "session2_table",
		Columns: []types.Column{{Name: "id", DataType: types.TypeInteger, PrimaryKey: true}},
	}
	table3 := &types.Table{
		Name: "session3_table",
		Columns: []types.Column{{Name: "id", DataType: types.TypeInteger, PrimaryKey: true}},
	}

	if err := session1.CreateTable(table1); err != nil {
		t.Fatalf("Session1 failed to create table: %v", err)
	}
	if err := session2.CreateTable(table2); err != nil {
		t.Fatalf("Session2 failed to create table: %v", err)
	}
	if err := session3.CreateTable(table3); err != nil {
		t.Fatalf("Session3 failed to create table: %v", err)
	}

	// Each session should only see their own pending table
	if tables := session1.GetTables(); len(tables) != 1 {
		t.Errorf("Session1 should see 1 table, got %d", len(tables))
	}
	if tables := session2.GetTables(); len(tables) != 1 {
		t.Errorf("Session2 should see 1 table, got %d", len(tables))
	}
	if tables := session3.GetTables(); len(tables) != 1 {
		t.Errorf("Session3 should see 1 table, got %d", len(tables))
	}

	// Session1 commits - others should now see session1's table
	_ = session1.Commit()

	if tables := session2.GetTables(); len(tables) != 2 {
		t.Errorf("Session2 should see 2 tables after session1 commit, got %d", len(tables))
	}

	// Session2 rolls back - session3 should not see session2's table
	_ = session2.Rollback()

	if tables := session3.GetTables(); len(tables) != 2 {
		t.Errorf("Session3 should see 2 tables (session1's committed + own pending), got %d", len(tables))
	}
	if _, ok := session3.GetTable("session2_table"); ok {
		t.Error("Session3 should NOT see session2's rolled back table")
	}

	// Session3 commits - all should see all committed tables
	_ = session3.Commit()

	if tables := session1.GetTables(); len(tables) != 2 {
		t.Errorf("Session1 should see 2 committed tables, got %d", len(tables))
	}
}
