package engine

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/llxgdtop/godb/internal/dberror"
	"github.com/llxgdtop/godb/internal/sql/types"
	"github.com/llxgdtop/godb/internal/storage"
)

// Key represents KV store keys
type Key struct {
	Type     KeyType
	TableName string
	RowKey   types.Value
}

type KeyType int

const (
	KeyTable KeyType = iota
	KeyRow
)

// Encode encodes the key to bytes
func (k *Key) Encode() []byte {
	var buf bytes.Buffer
	kc := storage.NewKeyCodec()

	switch k.Type {
	case KeyTable:
		buf.WriteByte(0x00) // Table prefix
		buf.Write(kc.EncodeString(k.TableName))
	case KeyRow:
		buf.WriteByte(0x01) // Row prefix
		buf.Write(kc.EncodeString(k.TableName))
		buf.Write(encodeValue(k.RowKey))
	}

	return buf.Bytes()
}

// TableKey returns the key for a table
func TableKey(name string) []byte {
	return (&Key{Type: KeyTable, TableName: name}).Encode()
}

// RowKey returns the key for a row
func RowKey(tableName string, pkValue types.Value) []byte {
	return (&Key{Type: KeyRow, TableName: tableName, RowKey: pkValue}).Encode()
}

func encodeValue(v types.Value) []byte {
	kc := storage.NewKeyCodec()
	switch v.Type {
	case types.TypeInteger:
		return kc.EncodeInt64(v.Int)
	case types.TypeString:
		return kc.EncodeString(v.Str)
	case types.TypeFloat:
		return kc.EncodeFloat64(v.Float)
	case types.TypeBoolean:
		if v.Bool {
			return []byte{1}
		}
		return []byte{0}
	default:
		return nil
	}
}

// KVEngine is a SQL engine backed by KV storage
type KVEngine struct {
	mu       sync.RWMutex
	storage  storage.Engine
	mvcc     *storage.Mvcc
	tables   map[string]*types.Table
	txn      *storage.MvccTransaction
	inTxn    bool
}

// NewKVEngine creates a new KV engine
func NewKVEngine(eng storage.Engine) *KVEngine {
	mvcc := storage.NewMvcc(eng)
	engine := &KVEngine{
		storage: eng,
		mvcc:    mvcc,
		tables:  make(map[string]*types.Table),
	}

	// Load existing tables
	engine.loadTables()

	return engine
}

// NewMemoryKVEngine creates a new KV engine with memory storage
func NewMemoryKVEngine() *KVEngine {
	return NewKVEngine(storage.NewMemoryEngine())
}

// NewDiskKVEngine creates a new KV engine with disk storage
func NewDiskKVEngine(dir string) (*KVEngine, error) {
	diskEngine, err := storage.NewDiskEngine(dir)
	if err != nil {
		return nil, err
	}
	return NewKVEngine(diskEngine), nil
}

func (e *KVEngine) loadTables() {
	it := e.storage.ScanPrefix([]byte{0x00}) // Table prefix
	for it.Next() {
		var table types.Table
		if err := json.Unmarshal(it.Value(), &table); err == nil {
			e.tables[table.Name] = &table
		}
	}
	it.Close()
}

// CreateTable creates a new table
func (e *KVEngine) CreateTable(schema *types.Table) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[schema.Name]; exists {
		return dberror.NewTableExistsError(schema.Name)
	}

	data, err := schema.Serialize()
	if err != nil {
		return err
	}

	key := TableKey(schema.Name)

	if e.inTxn && e.txn != nil {
		// In transaction - use it
		if err := e.txn.Set(key, data); err != nil {
			return err
		}
	} else {
		// Auto-commit mode: create temporary transaction
		txn := e.mvcc.Begin()
		if err := txn.Set(key, data); err != nil {
			txn.Rollback()
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	e.tables[schema.Name] = schema
	return nil
}

// GetTable returns a table by name
func (e *KVEngine) GetTable(name string) (*types.Table, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	table, ok := e.tables[name]
	return table, ok
}

// GetTables returns all tables
func (e *KVEngine) GetTables() map[string]*types.Table {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]*types.Table)
	for k, v := range e.tables {
		result[k] = v
	}
	return result
}

// Insert inserts a row into a table
func (e *KVEngine) Insert(tableName string, columns []string, values []types.Value) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	table, ok := e.tables[tableName]
	if !ok {
		return dberror.NewTableNotFoundError(tableName)
	}

	// Get primary key
	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return dberror.NewInternalError("table has no primary key")
	}

	// Build row key
	pkValue := values[pkIdx]
	rowKey := RowKey(tableName, pkValue)

	// Serialize row data
	rowData, err := types.SerializeRow(values)
	if err != nil {
		return err
	}

	// Use auto-commit mode if not in a transaction
	if e.inTxn && e.txn != nil {
		// Check for duplicate in current transaction
		existing, err := e.txn.Get(rowKey)
		if err != nil {
			return err
		}
		if existing != nil {
			return dberror.NewDuplicateKeyError()
		}
		return e.txn.Set(rowKey, rowData)
	}

	// Auto-commit mode: create temporary transaction
	txn := e.mvcc.Begin()

	// Check for duplicate
	existing, err := txn.Get(rowKey)
	if err != nil {
		txn.Rollback()
		return err
	}
	if existing != nil {
		txn.Rollback()
		return dberror.NewDuplicateKeyError()
	}

	// Insert the row
	if err := txn.Set(rowKey, rowData); err != nil {
		txn.Rollback()
		return err
	}

	// Commit the transaction
	return txn.Commit()
}

// CheckDuplicate checks if a row with the given primary key already exists
func (e *KVEngine) CheckDuplicate(tableName string, values []types.Value) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table, ok := e.tables[tableName]
	if !ok {
		return dberror.NewTableNotFoundError(tableName)
	}

	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return dberror.NewInternalError("table has no primary key")
	}

	pkValue := values[pkIdx]
	rowKey := RowKey(tableName, pkValue)

	// Use current transaction if in one
	if e.inTxn && e.txn != nil {
		existing, err := e.txn.Get(rowKey)
		if err != nil {
			return err
		}
		if existing != nil {
			return dberror.NewDuplicateKeyError()
		}
		return nil
	}

	// Auto-commit mode: create temporary transaction to check
	txn := e.mvcc.Begin()
	defer txn.Rollback()

	existing, err := txn.Get(rowKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return dberror.NewDuplicateKeyError()
	}
	return nil
}

// checkDuplicateInTxn checks for duplicates within a specific transaction
// The table parameter should be provided by the session (which includes pending tables)
func (e *KVEngine) checkDuplicateInTxn(table *types.Table, values []types.Value, txn *storage.MvccTransaction) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return dberror.NewInternalError("table has no primary key")
	}

	pkValue := values[pkIdx]
	rowKey := RowKey(table.Name, pkValue)

	existing, err := txn.Get(rowKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return dberror.NewDuplicateKeyError()
	}
	return nil
}

// Scan scans rows from a table
func (e *KVEngine) Scan(tableName string, filter func(types.Row) bool) ([]types.Row, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	_, ok := e.tables[tableName]
	if !ok {
		return nil, dberror.NewTableNotFoundError(tableName)
	}

	// Build prefix for table rows in MVCC: 0x03 (mvccKeyVersion) + 0x01 (row prefix) + EncodeString(tableName)
	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(tableName)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...) // Add MVCC version prefix

	var rows []types.Row

	if e.inTxn && e.txn != nil {
		// Use current transaction
		it := e.txn.Scan(mvccPrefix, nil)
		defer it.Close()

		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				rows = append(rows, row)
			}
		}
	} else {
		// When not in a transaction, create a temporary read transaction
		// to properly read MVCC-versioned data
		txn := e.mvcc.Begin()
		it := txn.Scan(mvccPrefix, nil)

		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				rows = append(rows, row)
			}
		}
		it.Close()
		txn.Rollback() // Read-only, just rollback to clean up
	}

	return rows, nil
}

// Update updates rows in a table
func (e *KVEngine) Update(tableName string, filter func(types.Row) bool, update func(types.Row) types.Row) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	table, ok := e.tables[tableName]
	if !ok {
		return 0, dberror.NewTableNotFoundError(tableName)
	}

	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return 0, dberror.NewInternalError("table has no primary key")
	}

	var count int64

	// Build prefix for table rows in MVCC: 0x03 (mvccKeyVersion) + 0x01 (row prefix) + EncodeString(tableName)
	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(tableName)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...)

	var toUpdate []struct {
		key   []byte
		newRow types.Row
	}

	if e.inTxn && e.txn != nil {
		// Use current transaction
		it := e.txn.Scan(mvccPrefix, nil)
		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				newRow := update(row)
				toUpdate = append(toUpdate, struct {
					key   []byte
					newRow types.Row
				}{key: it.Key(), newRow: newRow})
				count++
			}
		}
		it.Close()

		// Apply updates
		for _, u := range toUpdate {
			rowData, err := types.SerializeRow(u.newRow)
			if err != nil {
				return 0, err
			}
			_ = pkIdx  // Primary key update handling would be more complex
			_ = encodeValue(u.newRow[pkIdx])
			if err := e.txn.Set(u.key, rowData); err != nil {
				return 0, err
			}
		}
	} else {
		// Auto-commit mode: create temporary transaction
		txn := e.mvcc.Begin()

		// Scan for rows to update
		it := txn.Scan(mvccPrefix, nil)
		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				newRow := update(row)
				toUpdate = append(toUpdate, struct {
					key   []byte
					newRow types.Row
				}{key: it.Key(), newRow: newRow})
				count++
			}
		}
		it.Close()

		// Apply updates
		for _, u := range toUpdate {
			rowData, err := types.SerializeRow(u.newRow)
			if err != nil {
				txn.Rollback()
				return 0, err
			}
			_ = pkIdx  // Primary key update handling would be more complex
			_ = encodeValue(u.newRow[pkIdx])
			if err := txn.Set(u.key, rowData); err != nil {
				txn.Rollback()
				return 0, err
			}
		}

		// Commit the transaction
		if err := txn.Commit(); err != nil {
			return 0, err
		}
	}

	return count, nil
}

// Delete deletes rows from a table
func (e *KVEngine) Delete(tableName string, filter func(types.Row) bool) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	_, ok := e.tables[tableName]
	if !ok {
		return 0, dberror.NewTableNotFoundError(tableName)
	}

	var count int64

	// Build prefix for table rows in MVCC: 0x03 (mvccKeyVersion) + 0x01 (row prefix) + EncodeString(tableName)
	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(tableName)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...)

	var toDelete [][]byte

	if e.inTxn && e.txn != nil {
		// Use current transaction
		it := e.txn.Scan(mvccPrefix, nil)
		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				toDelete = append(toDelete, it.Key())
				count++
			}
		}
		it.Close()

		// Apply deletes
		for _, key := range toDelete {
			if err := e.txn.Delete(key); err != nil {
				return 0, err
			}
		}
	} else {
		// Auto-commit mode: create temporary transaction
		txn := e.mvcc.Begin()

		// Scan for rows to delete
		it := txn.Scan(mvccPrefix, nil)
		for it.Next() {
			row, err := types.DeserializeRow(it.Value())
			if err != nil {
				continue
			}
			if filter == nil || filter(row) {
				toDelete = append(toDelete, it.Key())
				count++
			}
		}
		it.Close()

		// Apply deletes
		for _, key := range toDelete {
			if err := txn.Delete(key); err != nil {
				txn.Rollback()
				return 0, err
			}
		}

		// Commit the transaction
		if err := txn.Commit(); err != nil {
			return 0, err
		}
	}

	return count, nil
}

// Begin starts a transaction
func (e *KVEngine) Begin() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.inTxn {
		return dberror.NewInternalError("already in transaction")
	}

	e.txn = e.mvcc.Begin()
	e.inTxn = true
	return nil
}

// Commit commits the current transaction
func (e *KVEngine) Commit() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.inTxn || e.txn == nil {
		return dberror.NewInternalError("not in transaction")
	}

	err := e.txn.Commit()
	e.txn = nil
	e.inTxn = false
	return err
}

// Rollback rolls back the current transaction
func (e *KVEngine) Rollback() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.inTxn || e.txn == nil {
		return dberror.NewInternalError("not in transaction")
	}

	err := e.txn.Rollback()
	e.txn = nil
	e.inTxn = false
	return err
}

// IsInTransaction returns whether the engine is in a transaction
func (e *KVEngine) IsInTransaction() bool {
	return e.inTxn
}

// GetTransactionVersion returns the current transaction version, or 0 if not in a transaction
func (e *KVEngine) GetTransactionVersion() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.txn != nil {
		return uint64(e.txn.Version())
	}
	return 0
}

// GetMvcc returns the MVCC layer (for session use)
func (e *KVEngine) GetMvcc() *storage.Mvcc {
	return e.mvcc
}

// createTableInTxn creates a table within a specific transaction
// Note: This does NOT add the table to e.tables - the caller (Session) is responsible
// for tracking the pending table and adding it to e.tables after commit
func (e *KVEngine) createTableInTxn(schema *types.Table, txn *storage.MvccTransaction) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[schema.Name]; exists {
		return dberror.NewTableExistsError(schema.Name)
	}

	data, err := schema.Serialize()
	if err != nil {
		return err
	}

	key := TableKey(schema.Name)
	if err := txn.Set(key, data); err != nil {
		return err
	}

	// Do NOT add to e.tables here - table should only be visible after commit
	// The Session will add it after successful commit
	return nil
}

// AddTable adds a table to the engine's in-memory cache
// This is called by Session.Commit() to make committed tables visible
func (e *KVEngine) AddTable(schema *types.Table) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tables[schema.Name] = schema
}

// insertInTxn inserts a row within a specific transaction
// The table parameter should be provided by the session (which includes pending tables)
func (e *KVEngine) insertInTxn(table *types.Table, columns []string, values []types.Value, txn *storage.MvccTransaction) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return dberror.NewInternalError("table has no primary key")
	}

	pkValue := values[pkIdx]
	rowKey := RowKey(table.Name, pkValue)

	rowData, err := types.SerializeRow(values)
	if err != nil {
		return err
	}

	existing, err := txn.Get(rowKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return dberror.NewDuplicateKeyError()
	}
	return txn.Set(rowKey, rowData)
}

// scanInTxn scans rows within a specific transaction
// The table parameter should be provided by the session (which includes pending tables)
func (e *KVEngine) scanInTxn(table *types.Table, filter func(types.Row) bool, txn *storage.MvccTransaction) ([]types.Row, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(table.Name)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...)

	var rows []types.Row

	it := txn.Scan(mvccPrefix, nil)
	defer it.Close()

	for it.Next() {
		row, err := types.DeserializeRow(it.Value())
		if err != nil {
			continue
		}
		if filter == nil || filter(row) {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// updateInTxn updates rows within a specific transaction
// The table parameter should be provided by the session (which includes pending tables)
func (e *KVEngine) updateInTxn(table *types.Table, filter func(types.Row) bool, update func(types.Row) types.Row, txn *storage.MvccTransaction) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pkIdx, hasPk := table.GetPrimaryKey()
	if !hasPk {
		return 0, dberror.NewInternalError("table has no primary key")
	}

	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(table.Name)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...)

	var toUpdate []struct {
		key   []byte
		newRow types.Row
	}
	var count int64

	it := txn.Scan(mvccPrefix, nil)
	for it.Next() {
		row, err := types.DeserializeRow(it.Value())
		if err != nil {
			continue
		}
		if filter == nil || filter(row) {
			newRow := update(row)
			toUpdate = append(toUpdate, struct {
				key   []byte
				newRow types.Row
			}{key: it.Key(), newRow: newRow})
			count++
		}
	}
	it.Close()

	for _, u := range toUpdate {
		rowData, err := types.SerializeRow(u.newRow)
		if err != nil {
			return 0, err
		}
		_ = pkIdx
		_ = encodeValue(u.newRow[pkIdx])
		if err := txn.Set(u.key, rowData); err != nil {
			return 0, err
		}
	}

	return count, nil
}

// deleteInTxn deletes rows within a specific transaction
// The table parameter should be provided by the session (which includes pending tables)
func (e *KVEngine) deleteInTxn(table *types.Table, filter func(types.Row) bool, txn *storage.MvccTransaction) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	kc := storage.NewKeyCodec()
	rowPrefix := append([]byte{0x01}, kc.EncodeString(table.Name)...)
	mvccPrefix := append([]byte{0x03}, rowPrefix...)

	var toDelete [][]byte
	var count int64

	it := txn.Scan(mvccPrefix, nil)
	for it.Next() {
		row, err := types.DeserializeRow(it.Value())
		if err != nil {
			continue
		}
		if filter == nil || filter(row) {
			toDelete = append(toDelete, it.Key())
			count++
		}
	}
	it.Close()

	for _, key := range toDelete {
		if err := txn.Delete(key); err != nil {
			return 0, err
		}
	}

	return count, nil
}
