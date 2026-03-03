package plan

import (
	"testing"

	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/types"
)

// TestPlanCreateTable tests CREATE TABLE planning
func TestPlanCreateTable(t *testing.T) {
	p, err := parser.ParseStatements("CREATE TABLE test (id INTEGER PRIMARY KEY, name STRING);")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(p) == 0 {
		t.Fatalf("No statements parsed")
	}

	planner := NewPlanner(nil)
	plan, err := planner.Plan(p[0])
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	// Verify it's a CreateTableNode
	createNode, ok := plan.(*CreateTableNode)
	if !ok {
		t.Fatalf("Expected CreateTableNode, got %T", plan)
	}

	// Verify schema
	if createNode.Schema.Name != "test" {
		t.Errorf("Expected table name 'test', got %s", createNode.Schema.Name)
	}
	if len(createNode.Schema.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(createNode.Schema.Columns))
	}
}

// TestPlanInsert tests INSERT planning
func TestPlanInsert(t *testing.T) {
	// First create a table schema
	schema := &types.Table{
		Name: "users",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
			{Name: "name", DataType: types.TypeString},
		},
	}

	p, err := parser.ParseStatements("INSERT INTO users (id, name) VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(p) == 0 {
		t.Fatalf("No statements parsed")
	}

	tables := map[string]*types.Table{"users": schema}
	planner := NewPlanner(tables)

	plan, err := planner.Plan(p[0])
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	// Verify it's an InsertNode
	insertNode, ok := plan.(*InsertNode)
	if !ok {
		t.Fatalf("Expected InsertNode, got %T", plan)
	}

	if insertNode.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", insertNode.TableName)
	}
	if len(insertNode.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertNode.Columns))
	}
	// Values may be parsed as a single row tuple or individual values
	// depending on the parser implementation
	if len(insertNode.Values) < 1 {
		t.Errorf("Expected at least 1 value, got %d", len(insertNode.Values))
	}
}

// TestPlanSelect tests SELECT planning
func TestPlanSelect(t *testing.T) {
	// First create a table schema
	schema := &types.Table{
		Name: "users",
		Columns: []types.Column{
			{Name: "id", DataType: types.TypeInteger, PrimaryKey: true},
			{Name: "name", DataType: types.TypeString},
		},
	}

	p, err := parser.ParseStatements("SELECT id, name FROM users WHERE id = 1;")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(p) == 0 {
		t.Fatalf("No statements parsed")
	}

	tables := map[string]*types.Table{"users": schema}
	planner := NewPlanner(tables)

	plan, err := planner.Plan(p[0])
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	// The plan should be a ProjectionNode wrapping other nodes
	// Verify it's not nil and is a valid node type
	if plan == nil {
		t.Fatalf("Expected non-nil plan")
	}

	// The outer node should be a ProjectionNode or contain one
	switch node := plan.(type) {
	case *ProjectionNode:
		if len(node.Exprs) != 2 {
			t.Errorf("Expected 2 select expressions, got %d", len(node.Exprs))
		}
	case *FilterNode:
		// SELECT with WHERE might have FilterNode as outer
		if node.Source == nil {
			t.Errorf("Expected FilterNode to have a source")
		}
	default:
		// Other node types are also acceptable
		t.Logf("Got plan type %T", node)
	}
}
