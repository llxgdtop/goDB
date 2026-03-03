package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/llxgdtop/godb/internal/sql/engine"
	"github.com/llxgdtop/godb/internal/sql/executor"
	"github.com/llxgdtop/godb/internal/sql/parser"
	"github.com/llxgdtop/godb/internal/sql/plan"
	"github.com/llxgdtop/godb/internal/sql/types"
)

const defaultPort = 8080
const responseEnd = "!!!end!!!"

func main() {
	// Get data directory from args or use default
	dataDir := "./data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}

	// Create KV engine
	var eng *engine.KVEngine
	var err error

	if dataDir == ":memory:" {
		eng = engine.NewMemoryKVEngine()
		fmt.Println("Using in-memory storage")
	} else {
		eng, err = engine.NewDiskKVEngine(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create disk engine: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Using disk storage at %s\n", dataDir)
	}

	// Start server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", defaultPort))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("godb server starts, listening on :%d\n", defaultPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
			continue
		}

		// Each connection gets its own session with isolated transaction state
		session := engine.NewSession(eng)
		go handleConnection(conn, session)
	}
}

func handleConnection(conn net.Conn, session *engine.Session) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	fmt.Printf("Client connected: %s\n", clientAddr)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Read command
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Read error from %s: %v\n", clientAddr, err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle quit command
		if strings.ToUpper(line) == "QUIT" || strings.ToUpper(line) == "EXIT" {
			// Rollback any active transaction before disconnecting
			if session.IsInTransaction() {
				session.Rollback()
			}
			sendResponse(writer, "Bye!")
			break
		}

		// Execute SQL
		response := executeSQL(session, line)

		// Send response
		sendResponse(writer, response)
	}

	// Clean up any active transaction on disconnect
	if session.IsInTransaction() {
		session.Rollback()
	}

	fmt.Printf("Client disconnected: %s\n", clientAddr)
}

func sendResponse(writer *bufio.Writer, response string) {
	// Send the response
	writer.WriteString(response)
	writer.WriteString("\n")
	// Send end marker
	writer.WriteString(responseEnd)
	writer.WriteString("\n")
	writer.Flush()
}

func executeSQL(session *engine.Session, input string) string {
	// Handle special commands
	upperInput := strings.ToUpper(input)
	if upperInput == "SHOW TABLES" {
		return formatTableList(session.GetTables())
	}
	if strings.HasPrefix(upperInput, "SHOW TABLE ") {
		// Extract table name, removing trailing semicolon if present
		tableName := strings.TrimSpace(input[11:])
		tableName = strings.TrimSuffix(tableName, ";")
		if tableName != "" {
			return formatTableInfo(session, tableName)
		}
	}

	// Parse
	stmts, err := parser.ParseStatements(input)
	if err != nil {
		return fmt.Sprintf("Parse error: %v", err)
	}

	var results []string
	for _, stmt := range stmts {
		// Plan
		planner := plan.NewPlanner(session.GetTables())
		node, err := planner.Plan(stmt)
		if err != nil {
			return fmt.Sprintf("Plan error: %v", err)
		}

		// Execute with session
		exec := executor.NewSessionExecutor(session)
		result, err := exec.Execute(node)
		if err != nil {
			return fmt.Sprintf("Execute error: %v", err)
		}

		results = append(results, formatResult(result))
	}

	return strings.Join(results, "\n")
}

func formatTableList(tables map[string]*types.Table) string {
	if len(tables) == 0 {
		return "Empty set"
	}

	var names []string
	for name := range tables {
		names = append(names, name)
	}

	// Calculate column width
	maxWidth := len("table_name")
	for _, name := range names {
		if len(name) > maxWidth {
			maxWidth = len(name)
		}
	}

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("%-*s", maxWidth, "table_name"))
	sb.WriteString("\n")

	// Separator
	sb.WriteString(strings.Repeat("-", maxWidth))
	sb.WriteString("\n")

	// Rows
	for _, name := range names {
		sb.WriteString(fmt.Sprintf("%-*s", maxWidth, name))
		sb.WriteString("\n")
	}

	// Footer
	sb.WriteString(fmt.Sprintf("(%d rows)", len(names)))

	return sb.String()
}

func formatTableInfo(session *engine.Session, tableName string) string {
	table, ok := session.GetTable(tableName)
	if !ok {
		return fmt.Sprintf("Table %s not found", tableName)
	}

	// Calculate column widths
	nameWidth := len("column_name")
	typeWidth := len("type")
	for _, col := range table.Columns {
		if len(col.Name) > nameWidth {
			nameWidth = len(col.Name)
		}
		if len(col.DataType.String()) > typeWidth {
			typeWidth = len(col.DataType.String())
		}
	}

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("%-*s | %-*s | nullable | primary_key",
		nameWidth, "column_name", typeWidth, "type"))
	sb.WriteString("\n")

	// Separator
	sb.WriteString(strings.Repeat("-", nameWidth))
	sb.WriteString("-+-")
	sb.WriteString(strings.Repeat("-", typeWidth))
	sb.WriteString("-+----------+------------")
	sb.WriteString("\n")

	// Rows
	for _, col := range table.Columns {
		sb.WriteString(fmt.Sprintf("%-*s | %-*s | %-8t | %t",
			nameWidth, col.Name,
			typeWidth, col.DataType.String(),
			col.Nullable,
			col.PrimaryKey))
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("(%d columns)", len(table.Columns)))

	return sb.String()
}

func formatResult(result *executor.Result) string {
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
			valStr := val.String()
			if len(valStr) > widths[i] {
				widths[i] = len(valStr)
			}
		}
	}

	var sb strings.Builder

	// Header
	for i, col := range result.Columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(fmt.Sprintf("%-*s", widths[i], col))
	}
	sb.WriteString("\n")

	// Separator
	for i := range result.Columns {
		if i > 0 {
			sb.WriteString("-+-")
		}
		sb.WriteString(strings.Repeat("-", widths[i]))
	}
	sb.WriteString("\n")

	// Rows
	for _, row := range result.Rows {
		for i, val := range row {
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(fmt.Sprintf("%-*s", widths[i], val.String()))
		}
		sb.WriteString("\n")
	}

	// Footer
	sb.WriteString(fmt.Sprintf("(%d rows)", len(result.Rows)))

	return sb.String()
}
