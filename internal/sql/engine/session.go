package engine

import (
	"sync"

	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/types"
	"github.com/llxgdtop/godb/internal/storage"
)

// Session represents a client session with its own transaction context
type Session struct {
	engine        *KVEngine
	txn           *storage.MvccTransaction
	inTxn         bool
	pendingTables map[string]*types.Table // Tables created in current uncommitted transaction
	mu            sync.RWMutex
}

// NewSession creates a new session for a connection
func NewSession(engine *KVEngine) *Session {
	return &Session{
		engine:        engine,
		pendingTables: make(map[string]*types.Table),
	}
}

// GetEngine returns the underlying engine
func (s *Session) GetEngine() *KVEngine {
	return s.engine
}

// CreateTable creates a table (uses session transaction if active)
func (s *Session) CreateTable(schema *types.Table) error {
	if s.inTxn && s.txn != nil {
		// Check if table already exists in engine's committed tables
		if _, exists := s.engine.GetTable(schema.Name); exists {
			return dberror.NewTableExistsError(schema.Name)
		}
		// Check if table already exists in pending tables
		s.mu.RLock()
		if _, exists := s.pendingTables[schema.Name]; exists {
			s.mu.RUnlock()
			return dberror.NewTableExistsError(schema.Name)
		}
		s.mu.RUnlock()

		// Create table in transaction storage
		if err := s.engine.createTableInTxn(schema, s.txn); err != nil {
			return err
		}

		// Track as pending table (only visible to this session until commit)
		s.mu.Lock()
		s.pendingTables[schema.Name] = schema
		s.mu.Unlock()
		return nil
	}
	return s.engine.CreateTable(schema)
}

// GetTable returns a table by name
func (s *Session) GetTable(name string) (*types.Table, bool) {
	// First check pending tables (this session's uncommitted tables)
	s.mu.RLock()
	if table, ok := s.pendingTables[name]; ok {
		s.mu.RUnlock()
		return table, true
	}
	s.mu.RUnlock()

	// Then check engine's committed tables
	return s.engine.GetTable(name)
}

// GetTables returns all tables (committed + this session's pending tables)
func (s *Session) GetTables() map[string]*types.Table {
	committed := s.engine.GetTables()

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Merge with pending tables
	result := make(map[string]*types.Table)
	for k, v := range committed {
		result[k] = v
	}
	for k, v := range s.pendingTables {
		result[k] = v // Pending tables override committed (for same name)
	}
	return result
}

// Insert inserts a row
func (s *Session) Insert(tableName string, columns []string, values []types.Value) error {
	// Look up table (includes pending tables for this session)
	table, ok := s.GetTable(tableName)
	if !ok {
		return dberror.NewTableNotFoundError(tableName)
	}

	if s.inTxn && s.txn != nil {
		return s.engine.insertInTxn(table, columns, values, s.txn)
	}
	return s.engine.Insert(tableName, columns, values)
}

// CheckDuplicate checks if a row with the given primary key already exists
func (s *Session) CheckDuplicate(tableName string, values []types.Value) error {
	// Look up table (includes pending tables for this session)
	table, ok := s.GetTable(tableName)
	if !ok {
		return dberror.NewTableNotFoundError(tableName)
	}

	if s.inTxn && s.txn != nil {
		return s.engine.checkDuplicateInTxn(table, values, s.txn)
	}
	// For non-transaction mode, use the table name (engine will look up from committed tables)
	return s.engine.CheckDuplicate(tableName, values)
}

// Scan scans rows from a table
func (s *Session) Scan(tableName string, filter func(types.Row) bool) ([]types.Row, error) {
	// Look up table (includes pending tables for this session)
	table, ok := s.GetTable(tableName)
	if !ok {
		return nil, dberror.NewTableNotFoundError(tableName)
	}

	if s.inTxn && s.txn != nil {
		return s.engine.scanInTxn(table, filter, s.txn)
	}
	return s.engine.Scan(tableName, filter)
}

// Update updates rows in a table
func (s *Session) Update(tableName string, filter func(types.Row) bool, update func(types.Row) types.Row) (int64, error) {
	// Look up table (includes pending tables for this session)
	table, ok := s.GetTable(tableName)
	if !ok {
		return 0, dberror.NewTableNotFoundError(tableName)
	}

	if s.inTxn && s.txn != nil {
		return s.engine.updateInTxn(table, filter, update, s.txn)
	}
	return s.engine.Update(tableName, filter, update)
}

// Delete deletes rows from a table
func (s *Session) Delete(tableName string, filter func(types.Row) bool) (int64, error) {
	// Look up table (includes pending tables for this session)
	table, ok := s.GetTable(tableName)
	if !ok {
		return 0, dberror.NewTableNotFoundError(tableName)
	}

	if s.inTxn && s.txn != nil {
		return s.engine.deleteInTxn(table, filter, s.txn)
	}
	return s.engine.Delete(tableName, filter)
}

// Begin starts a transaction for this session
func (s *Session) Begin() error {
	if s.inTxn {
		return dberror.NewInternalError("already in transaction")
	}

	s.txn = s.engine.mvcc.Begin()
	s.inTxn = true
	s.mu.Lock()
	s.pendingTables = make(map[string]*types.Table)
	s.mu.Unlock()
	return nil
}

// Commit commits the current transaction
func (s *Session) Commit() error {
	if !s.inTxn || s.txn == nil {
		return dberror.NewInternalError("not in transaction")
	}

	err := s.txn.Commit()
	if err != nil {
		return err
	}

	// After successful commit, make pending tables visible to all sessions
	s.mu.Lock()
	pendingTables := s.pendingTables
	s.pendingTables = make(map[string]*types.Table)
	s.mu.Unlock()

	// Add tables to engine's cache (using proper synchronization)
	for _, table := range pendingTables {
		s.engine.AddTable(table)
	}

	s.txn = nil
	s.inTxn = false
	return nil
}

// Rollback rolls back the current transaction
func (s *Session) Rollback() error {
	if !s.inTxn || s.txn == nil {
		return dberror.NewInternalError("not in transaction")
	}

	err := s.txn.Rollback()

	// Discard pending tables regardless of rollback error
	s.mu.Lock()
	s.pendingTables = make(map[string]*types.Table)
	s.mu.Unlock()

	s.txn = nil
	s.inTxn = false
	return err
}

// IsInTransaction returns whether the session is in a transaction
func (s *Session) IsInTransaction() bool {
	return s.inTxn
}

// GetTransactionVersion returns the current transaction version
func (s *Session) GetTransactionVersion() uint64 {
	if s.txn != nil {
		return uint64(s.txn.Version())
	}
	return 0
}
