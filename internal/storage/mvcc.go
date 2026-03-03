package storage

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/llxgdtop/godb/internal/dberror"
)

// MVCC key type prefixes
const (
	mvccKeyVersion     byte = 0x03 // MVCC versioned keys
	mvccKeyLock        byte = 0x04 // Lock keys
	mvccKeyTxnActive   byte = 0x05 // Active transaction markers
	mvccKeyNextVersion byte = 0x06 // Next version counter
	mvccKeyTxnWrite    byte = 0x07 // Transaction write markers
)

// Mvcc implements multi-version concurrency control
type Mvcc struct {
	engine          Engine
	version         atomic.Uint64
	mu              sync.RWMutex
	activeTxns      map[uint64]bool // Track globally active transactions
}

// NewMvcc creates a new MVCC layer
func NewMvcc(engine Engine) *Mvcc {
	m := &Mvcc{
		engine:     engine,
		activeTxns: make(map[uint64]bool),
	}

	// Load or initialize the next version counter from storage
	nextVersionKey := NextVersionKey()
	if val, err := engine.Get(nextVersionKey); err == nil && val != nil {
		if len(val) == 8 {
			version := binary.BigEndian.Uint64(val)
			m.version.Store(version)
		}
	}

	return m
}

// MvccTransaction represents a transaction in the MVCC layer
type MvccTransaction struct {
	engine        Engine
	mvcc          *Mvcc
	version       uint64
	activeTxns    map[uint64]bool // Snapshot of active transactions at start
	writes        map[string][]byte
	deletes       map[string]bool
	readSet       map[string]bool
	writeSet      map[string]bool
	committed     bool
	rolledBack    bool
}

// Begin starts a new transaction
func (m *Mvcc) Begin() *MvccTransaction {
	m.mu.Lock()
	newVersion := m.version.Add(1)

	// Persist the next version counter to storage
	nextVersionKey := NextVersionKey()
	nextVersionBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(nextVersionBuf, newVersion+1)
	m.engine.Set(nextVersionKey, nextVersionBuf)

	// Create snapshot of currently active transactions
	activeTxnsSnapshot := make(map[uint64]bool, len(m.activeTxns))
	for version := range m.activeTxns {
		activeTxnsSnapshot[version] = true
	}

	// Mark this transaction as active
	m.activeTxns[newVersion] = true
	m.mu.Unlock()

	// Mark transaction as active in persistent storage
	txnKey := append([]byte{mvccKeyTxnActive}, encodeUint64(newVersion)...)
	m.engine.Set(txnKey, []byte{1})

	return &MvccTransaction{
		engine:     m.engine,
		mvcc:       m,
		version:    newVersion,
		activeTxns: activeTxnsSnapshot,
		writes:     make(map[string][]byte),
		deletes:    make(map[string]bool),
		readSet:    make(map[string]bool),
		writeSet:   make(map[string]bool),
	}
}

// Version returns the transaction version
func (t *MvccTransaction) Version() uint64 {
	return t.version
}

// Get retrieves a value with MVCC visibility
func (t *MvccTransaction) Get(key []byte) ([]byte, error) {
	keyStr := string(key)

	// Check local writes first
	if val, ok := t.writes[keyStr]; ok {
		return val, nil
	}

	// Check if deleted in this transaction
	if t.deletes[keyStr] {
		return nil, nil
	}

	// Track read set
	t.readSet[keyStr] = true

	// Look for the most recent visible version
	mvccKey := append([]byte{mvccKeyVersion}, key...)

	// Scan all versions of this key
	it := t.engine.ScanPrefix(mvccKey)
	defer it.Close()

	var latestValue []byte
	var latestVersion uint64
	found := false

	for it.Next() {
		mk := DecodeMvccKey(it.Key())
		if !bytes.Equal(mk.data, key) {
			continue
		}
		if mk.keyType == mvccKeyVersion && t.isVisible(mk.version) {
			if !found || mk.version > latestVersion {
				latestVersion = mk.version
				latestValue = it.Value()
				found = true
			}
		}
	}

	if !found {
		return nil, nil
	}

	return latestValue, nil
}

// Set writes a value with MVCC versioning
// Writes immediately to storage so other transactions can see based on visibility rules
func (t *MvccTransaction) Set(key, value []byte) error {
	keyStr := string(key)

	// Check for write conflict: if any active transaction has written to this key
	// We need to check the storage for any versions written by transactions in our activeTxns
	if err := t.checkWriteConflict(key); err != nil {
		return err
	}

	// Track in local writes for our own visibility and rollback
	t.writes[keyStr] = value
	t.writeSet[keyStr] = true
	delete(t.deletes, keyStr)

	// Write to storage immediately with our version
	mvccKey := EncodeMvccKey(key, t.version)
	t.engine.Set(mvccKey, value)

	// Write transaction write marker for conflict detection by other transactions
	txnWriteKey := TxnWriteKey(t.version, key)
	t.engine.Set(txnWriteKey, []byte{1})

	return nil
}

// Delete marks a key for deletion
// Writes tombstone immediately to storage
func (t *MvccTransaction) Delete(key []byte) error {
	keyStr := string(key)

	// Check for write conflict
	if err := t.checkWriteConflict(key); err != nil {
		return err
	}

	// Track in local deletes
	t.deletes[keyStr] = true
	t.writeSet[keyStr] = true
	delete(t.writes, keyStr)

	// Write tombstone to storage immediately
	mvccKey := EncodeMvccKey(key, t.version)
	t.engine.Set(mvccKey, nil)

	// Write transaction write marker
	txnWriteKey := TxnWriteKey(t.version, key)
	t.engine.Set(txnWriteKey, []byte{1})

	return nil
}

// checkWriteConflict checks if there's a write conflict for this key
// A conflict occurs if the latest version of this key is NOT visible to this transaction
// This matches RustDB's approach: scan version history and check visibility of latest version
func (t *MvccTransaction) checkWriteConflict(key []byte) error {
	// Build the MVCC key prefix for this key
	mvccKeyPrefix := append([]byte{mvccKeyVersion}, key...)

	// Find the latest version of this key by scanning all versions
	it := t.engine.ScanPrefix(mvccKeyPrefix)
	defer it.Close()

	var latestVersion uint64
	var found bool

	for it.Next() {
		mk := DecodeMvccKey(it.Key())
		if !bytes.Equal(mk.data, key) {
			continue
		}
		if mk.keyType == mvccKeyVersion {
			// Keep track of the latest version
			if !found || mk.version > latestVersion {
				latestVersion = mk.version
				found = true
			}
		}
	}

	// If no version exists, no conflict
	if !found {
		return nil
	}

	// Check if the latest version is visible to this transaction
	// If it's NOT visible, then there's a conflict (someone else modified it after our snapshot)
	if !t.isVisible(latestVersion) {
		return dberror.NewWriteConflictError()
	}

	return nil
}

// MvccKey represents a decoded MVCC key
type MvccKey struct {
	keyType byte
	version uint64
	data    []byte
}

// Encode encodes an MvccKey to bytes
func (k *MvccKey) Encode() []byte {
	switch k.keyType {
	case mvccKeyNextVersion:
		return []byte{mvccKeyNextVersion}
	case mvccKeyTxnActive:
		buf := make([]byte, 1+8)
		buf[0] = mvccKeyTxnActive
		binary.BigEndian.PutUint64(buf[1:], k.version)
		return buf
	case mvccKeyTxnWrite:
		buf := make([]byte, 1+8+len(k.data))
		buf[0] = mvccKeyTxnWrite
		binary.BigEndian.PutUint64(buf[1:], k.version)
		copy(buf[9:], k.data)
		return buf
	case mvccKeyVersion:
		buf := make([]byte, 1+len(k.data)+8)
		buf[0] = mvccKeyVersion
		copy(buf[1:], k.data)
		binary.BigEndian.PutUint64(buf[1+len(k.data):], k.version)
		return buf
	default:
		return []byte{k.keyType}
	}
}

// NextVersionKey returns the key for the next version counter
func NextVersionKey() []byte {
	return []byte{mvccKeyNextVersion}
}

// TxnActiveKey returns the key for a transaction active marker
func TxnActiveKey(version uint64) []byte {
	buf := make([]byte, 1+8)
	buf[0] = mvccKeyTxnActive
	binary.BigEndian.PutUint64(buf[1:], version)
	return buf
}

// TxnWriteKey returns the key for a transaction write marker
func TxnWriteKey(version uint64, key []byte) []byte {
	buf := make([]byte, 1+8+len(key))
	buf[0] = mvccKeyTxnWrite
	binary.BigEndian.PutUint64(buf[1:], version)
	copy(buf[9:], key)
	return buf
}

// VersionKey returns the MVCC versioned key for a user key
func VersionKey(key []byte, version uint64) []byte {
	return EncodeMvccKey(key, version)
}

// Version extracts the version from an MVCC key
func Version(key []byte) uint64 {
	mk := DecodeMvccKey(key)
	return mk.version
}

// DecodeMvccKey decodes an MVCC key
func DecodeMvccKey(key []byte) MvccKey {
	if len(key) < 1 {
		return MvccKey{}
	}

	keyType := key[0]

	switch keyType {
	case mvccKeyVersion:
		// MVCC versioned key format: 0x03 + data + version
		if len(key) > 9 {
			dataLen := len(key) - 8
			return MvccKey{
				keyType: keyType,
				data:    key[1:dataLen],
				version: binary.BigEndian.Uint64(key[dataLen:]),
			}
		}
	case mvccKeyTxnActive:
		// TxnActive key format: 0x05 + version (8 bytes)
		if len(key) >= 9 {
			return MvccKey{
				keyType: keyType,
				version: binary.BigEndian.Uint64(key[1:9]),
			}
		}
	case mvccKeyTxnWrite:
		// TxnWrite key format: 0x07 + version (8 bytes) + data
		if len(key) >= 9 {
			return MvccKey{
				keyType: keyType,
				version: binary.BigEndian.Uint64(key[1:9]),
				data:    key[9:],
			}
		}
	case mvccKeyNextVersion:
		// NextVersion key format: just 0x06
		return MvccKey{
			keyType: keyType,
		}
	}

	return MvccKey{
		keyType: keyType,
		data:    key[1:],
	}
}

// EncodeMvccKey encodes a key with version
func EncodeMvccKey(key []byte, version uint64) []byte {
	buf := make([]byte, 1+len(key)+8)
	buf[0] = mvccKeyVersion
	copy(buf[1:], key)
	binary.BigEndian.PutUint64(buf[1+len(key):], version)
	return buf
}

// isVisible checks if a version is visible to this transaction
// A version is visible if:
// 1. It's NOT from a transaction that was active when this transaction started (dirty read prevention)
// 2. AND the version <= this transaction's version (snapshot isolation)
func (t *MvccTransaction) isVisible(version uint64) bool {
	// If this version was created by a transaction that was active when we started,
	// it's not visible (prevents dirty reads from transactions that were already running)
	if t.activeTxns[version] {
		return false
	}
	// Snapshot isolation: only see versions <= our version
	return version <= t.version
}

// Scan scans a range of keys with MVCC visibility
// The start and end parameters are raw storage key ranges (e.g., []byte{0x03} to []byte{0x04} to scan all MVCC keys)
// Returns the LATEST visible version for each key
func (t *MvccTransaction) Scan(start, end []byte) EngineIterator {
	// Collect all visible key-value pairs, keeping only the LATEST visible version
	// for each key
	type versionedValue struct {
		version uint64
		value   []byte
	}

	// Map from key string to latest visible version and value
	visibleKeys := make(map[string]versionedValue)

	// Scan the raw storage range directly
	var it EngineIterator
	if end == nil {
		it = t.engine.ScanPrefix(start)
	} else {
		it = t.engine.Scan(start, end)
	}
	defer it.Close()

	for it.Next() {
		mk := DecodeMvccKey(it.Key())
		if mk.keyType == mvccKeyVersion {
			if t.isVisible(mk.version) {
				keyStr := string(mk.data)
				existing, exists := visibleKeys[keyStr]
				// Keep this version if it's newer than what we have
				if !exists || mk.version > existing.version {
					visibleKeys[keyStr] = versionedValue{
						version: mk.version,
						value:   it.Value(),
					}
				}
			}
		}
	}

	// Convert map to sorted slices
	var keys [][]byte
	var values [][]byte
	for keyStr, vv := range visibleKeys {
		keys = append(keys, []byte(keyStr))
		values = append(values, vv.value)
	}

	// Sort keys lexicographically
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Reorder values to match sorted keys
	sortedValues := make([][]byte, len(keys))
	for i, k := range keys {
		sortedValues[i] = visibleKeys[string(k)].value
	}

	return &MvccIterator{
		keys:   keys,
		values: sortedValues,
		pos:    -1,
	}
}

// MvccIterator implements EngineIterator for MVCC scans
type MvccIterator struct {
	keys   [][]byte
	values [][]byte
	pos    int
}

func (it *MvccIterator) Next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

func (it *MvccIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return it.keys[it.pos]
}

func (it *MvccIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.values) {
		return nil
	}
	return it.values[it.pos]
}

func (it *MvccIterator) Close() {
	// Nothing to close for in-memory iterator
}

func (it *MvccIterator) Err() error {
	return nil
}

// Commit commits the transaction
func (t *MvccTransaction) Commit() error {
	if t.committed || t.rolledBack {
		return nil
	}

	// Data is already written to storage on Set/Delete
	// Just need to clean up transaction markers

	// Remove transaction write markers
	for keyStr := range t.writeSet {
		key := []byte(keyStr)
		txnWriteKey := TxnWriteKey(t.version, key)
		t.engine.Delete(txnWriteKey)
	}

	// Remove transaction from active set
	t.mvcc.mu.Lock()
	delete(t.mvcc.activeTxns, t.version)
	t.mvcc.mu.Unlock()

	// Remove transaction active marker from persistent storage
	txnKey := append([]byte{mvccKeyTxnActive}, encodeUint64(t.version)...)
	t.engine.Delete(txnKey)

	t.committed = true
	return nil
}

// Rollback rolls back the transaction
func (t *MvccTransaction) Rollback() error {
	if t.committed || t.rolledBack {
		return nil
	}

	// Delete all written data from storage
	for keyStr := range t.writes {
		key := []byte(keyStr)
		mvccKey := EncodeMvccKey(key, t.version)
		t.engine.Delete(mvccKey)
	}

	// Delete tombstones
	for keyStr := range t.deletes {
		key := []byte(keyStr)
		mvccKey := EncodeMvccKey(key, t.version)
		t.engine.Delete(mvccKey)
	}

	// Remove transaction write markers
	for keyStr := range t.writeSet {
		key := []byte(keyStr)
		txnWriteKey := TxnWriteKey(t.version, key)
		t.engine.Delete(txnWriteKey)
	}

	// Remove transaction from active set
	t.mvcc.mu.Lock()
	delete(t.mvcc.activeTxns, t.version)
	t.mvcc.mu.Unlock()

	// Remove transaction active marker from persistent storage
	txnKey := append([]byte{mvccKeyTxnActive}, encodeUint64(t.version)...)
	t.engine.Delete(txnKey)

	t.rolledBack = true
	return nil
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// ScanPrefix scans keys with a specific prefix
// The prefix parameter is a raw storage key prefix
// Returns the LATEST visible version for each key
func (t *MvccTransaction) ScanPrefix(prefix []byte) EngineIterator {
	// Collect all visible key-value pairs, keeping only the LATEST visible version
	type versionedValue struct {
		version uint64
		value   []byte
	}

	// Map from key string to latest visible version and value
	visibleKeys := make(map[string]versionedValue)

	// Scan with the raw storage prefix directly
	it := t.engine.ScanPrefix(prefix)
	defer it.Close()

	for it.Next() {
		mk := DecodeMvccKey(it.Key())
		if mk.keyType == mvccKeyVersion {
			if t.isVisible(mk.version) {
				keyStr := string(mk.data)
				existing, exists := visibleKeys[keyStr]
				// Keep this version if it's newer than what we have
				if !exists || mk.version > existing.version {
					visibleKeys[keyStr] = versionedValue{
						version: mk.version,
						value:   it.Value(),
					}
				}
			}
		}
	}

	// Convert map to slices and sort
	var keys [][]byte
	var values [][]byte
	for keyStr, vv := range visibleKeys {
		keys = append(keys, []byte(keyStr))
		values = append(values, vv.value)
	}

	// Sort keys lexicographically
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Reorder values to match sorted keys
	sortedValues := make([][]byte, len(keys))
	for i, k := range keys {
		sortedValues[i] = visibleKeys[string(k)].value
	}

	return &MvccIterator{
		keys:   keys,
		values: sortedValues,
		pos:    -1,
	}
}
