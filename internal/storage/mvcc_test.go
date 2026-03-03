package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/llxgdtop/godb/internal/dberror"
)

// ScanResult represents a key-value pair from scan
type ScanResult struct {
	Key   []byte
	Value []byte
}

// collectScanResults collects all results from an iterator
func collectScanResults(it EngineIterator) []ScanResult {
	var results []ScanResult
	for it.Next() {
		// Skip tombstones (nil or empty values from deletes)
		if it.Value() == nil || len(it.Value()) == 0 {
			continue
		}
		results = append(results, ScanResult{
			Key:   it.Key(),
			Value: it.Value(),
		})
	}
	it.Close()
	return results
}

// isTombstone checks if a value represents a deleted key
func isTombstone(val []byte) bool {
	return val == nil || len(val) == 0
}

// ============================================================================
// 1. Basic Operations: Get, Set, Delete within a transaction
// ============================================================================

func testBasicOperations(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Start transaction and perform operations
	tx := mvcc.Begin()
	if err := tx.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	if err := tx.Set([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}
	if err := tx.Set([]byte("key2"), []byte("val3")); err != nil {
		t.Fatalf("Set key2 update failed: %v", err)
	}
	if err := tx.Set([]byte("key3"), []byte("val4")); err != nil {
		t.Fatalf("Set key3 failed: %v", err)
	}
	if err := tx.Delete([]byte("key3")); err != nil {
		t.Fatalf("Delete key3 failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify results in new transaction
	tx1 := mvcc.Begin()

	val, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("key1 = %s, want val1", val)
	}

	val, err = tx1.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val3")) {
		t.Errorf("key2 = %s, want val3", val)
	}

	// key3 was deleted - should return nil/empty (tombstone)
	val, err = tx1.Get([]byte("key3"))
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if !isTombstone(val) {
		t.Errorf("key3 = %s, want nil/empty (deleted)", val)
	}
}

func TestBasicOperations_MemoryEngine(t *testing.T) {
	testBasicOperations(t, NewMemoryEngine())
}

func TestBasicOperations_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testBasicOperations(t, engine)
}

// ============================================================================
// 2. Read Isolation: transaction sees snapshot at start time
// RustDB visibility rules:
// 1. If version is in activeTxns, it's NOT visible (dirty read prevention)
// 2. Otherwise, version is visible if version <= transaction.version (snapshot isolation)
// ============================================================================

func testReadIsolation(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key2"), []byte("val3"))
	tx.Set([]byte("key3"), []byte("val4"))
	tx.Commit()

	// tx1 starts - should see snapshot at this point
	tx1 := mvcc.Begin()

	// tx2 starts AFTER tx1, so tx1 is in tx2's ActiveTxns
	// But tx2 is NOT in tx1's ActiveTxns (tx1 started before tx2)
	tx2 := mvcc.Begin()
	tx2.Set([]byte("key1"), []byte("val2"))

	// tx3 starts AFTER tx2, modifies and commits
	tx3 := mvcc.Begin()
	tx3.Set([]byte("key2"), []byte("val4"))
	tx3.Delete([]byte("key3"))
	tx3.Commit()

	// Now test visibility from tx1's perspective:
	// tx1's ActiveTxns = {} (empty - no active txns when tx1 started)
	// But tx2.version > tx1.version, so tx1 should NOT see tx2's changes
	// And tx3.version > tx1.version, so tx1 should NOT see tx3's changes

	// tx1 should see original value (snapshot isolation)
	val, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	// tx1 should see original value because tx2.version > tx1.version
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("key1 = %s, want val1 (snapshot isolation)", val)
	}

	// tx1 should see original value (tx3.version > tx1.version)
	val, err = tx1.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val3")) {
		t.Errorf("key2 = %s, want val3 (snapshot isolation)", val)
	}

	// tx1 should see original value (tx3.version > tx1.version)
	val, err = tx1.Get([]byte("key3"))
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val4")) {
		t.Errorf("key3 = %s, want val4 (snapshot isolation)", val)
	}
}

func TestReadIsolation_MemoryEngine(t *testing.T) {
	testReadIsolation(t, NewMemoryEngine())
}

func TestReadIsolation_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testReadIsolation(t, engine)
}

// ============================================================================
// 3. Dirty Read Prevention: uncommitted changes not visible to other transactions
// This test demonstrates that if tx2 starts AFTER tx1, then tx2's changes
// are visible to tx1 (since tx2 is not in tx1's ActiveTxns).
// To properly test dirty read prevention, we need tx1 to start AFTER tx2.
// ============================================================================

func testDirtyReadPrevention(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key3"), []byte("val3"))
	tx.Commit()

	// tx2 starts first and makes uncommitted changes
	tx2 := mvcc.Begin()
	tx2.Set([]byte("key1"), []byte("val1-1"))

	// tx1 starts AFTER tx2, so tx2 is in tx1's ActiveTxns
	tx1 := mvcc.Begin()

	// tx1 should NOT see tx2's uncommitted change
	// because tx2's version is in tx1's ActiveTxns
	val, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("key1 = %s, want val1 (dirty read prevented)", val)
	}
}

func TestDirtyReadPrevention_MemoryEngine(t *testing.T) {
	testDirtyReadPrevention(t, NewMemoryEngine())
}

func TestDirtyReadPrevention_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testDirtyReadPrevention(t, engine)
}

// ============================================================================
// 4. Unrepeatable Read Prevention: same query returns same results within transaction
// ============================================================================

func testUnrepeatableReadPrevention(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key3"), []byte("val3"))
	tx.Commit()

	// tx2 starts first and will make changes
	tx2 := mvcc.Begin()

	// tx1 starts AFTER tx2, so tx2 is in tx1's ActiveTxns
	tx1 := mvcc.Begin()

	// tx1 reads key1 first time - should see original
	val1, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("First Get key1 failed: %v", err)
	}
	if !bytes.Equal(val1, []byte("val1")) {
		t.Errorf("First read: key1 = %s, want val1", val1)
	}

	// tx2 makes changes
	tx2.Set([]byte("key1"), []byte("val1-1"))

	// tx2 commits - but tx2 is still in tx1's ActiveTxns (captured at begin)
	tx2.Commit()

	// tx1 reads key1 again
	// tx2's version should still be hidden because tx2 was in tx1's ActiveTxns
	val2, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Second Get key1 failed: %v", err)
	}
	if !bytes.Equal(val2, []byte("val1")) {
		t.Errorf("Second read: key1 = %s, want val1 (unrepeatable read prevented)", val2)
	}
}

func TestUnrepeatableReadPrevention_MemoryEngine(t *testing.T) {
	testUnrepeatableReadPrevention(t, NewMemoryEngine())
}

func TestUnrepeatableReadPrevention_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testUnrepeatableReadPrevention(t, engine)
}

// ============================================================================
// 5. Phantom Read Prevention: new rows from other transactions not visible
// ============================================================================

func testPhantomReadPrevention(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key3"), []byte("val3"))
	tx.Commit()

	// tx2 starts first and will make changes
	tx2 := mvcc.Begin()

	// tx1 starts AFTER tx2, so tx2 is in tx1's ActiveTxns
	tx1 := mvcc.Begin()

	// First scan - should see 3 keys
	// Note: Scan uses MVCC-encoded keys, so we scan from 0x03 to 0x04
	iter1 := tx1.Scan([]byte{0x03}, []byte{0x04})
	results1 := collectScanResults(iter1)
	if len(results1) != 3 {
		t.Errorf("First scan: got %d results, want 3", len(results1))
	}

	// tx2 modifies and adds a key, then commits
	tx2.Set([]byte("key2"), []byte("val2-1"))
	tx2.Set([]byte("key4"), []byte("val4"))
	tx2.Commit()

	// Second scan by tx1 - should still see 3 keys
	// tx2's changes should not be visible because tx2 was in tx1's ActiveTxns
	iter2 := tx1.Scan([]byte{0x03}, []byte{0x04})
	results2 := collectScanResults(iter2)
	if len(results2) != 3 {
		t.Errorf("Second scan: got %d results, want 3 (phantom read prevented)", len(results2))
	}

	// Verify values are unchanged
	for _, r := range results2 {
		if bytes.Equal(r.Key, []byte("key1")) && !bytes.Equal(r.Value, []byte("val1")) {
			t.Errorf("key1 = %s, want val1", r.Value)
		}
		if bytes.Equal(r.Key, []byte("key2")) && !bytes.Equal(r.Value, []byte("val2")) {
			t.Errorf("key2 = %s, want val2", r.Value)
		}
		if bytes.Equal(r.Key, []byte("key3")) && !bytes.Equal(r.Value, []byte("val3")) {
			t.Errorf("key3 = %s, want val3", r.Value)
		}
	}
}

func TestPhantomReadPrevention_MemoryEngine(t *testing.T) {
	testPhantomReadPrevention(t, NewMemoryEngine())
}

func TestPhantomReadPrevention_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testPhantomReadPrevention(t, engine)
}

// ============================================================================
// 6. Write Conflict Detection: concurrent writes to same key detected
// ============================================================================

func testWriteConflictDetection(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key2"), []byte("val3"))
	tx.Set([]byte("key3"), []byte("val4"))
	tx.Set([]byte("key4"), []byte("val5"))
	tx.Commit()

	// tx1 starts first
	tx1 := mvcc.Begin()

	// tx2 starts after tx1 (tx1 in tx2's ActiveTxns)
	tx2 := mvcc.Begin()

	// tx1 modifies key1
	if err := tx1.Set([]byte("key1"), []byte("val1-1")); err != nil {
		t.Fatalf("tx1 Set key1 failed: %v", err)
	}
	if err := tx1.Set([]byte("key1"), []byte("val1-2")); err != nil {
		t.Fatalf("tx1 Set key1 update failed: %v", err)
	}

	// tx2 tries to modify same key - should get conflict
	// because tx1 (which modified key1) is in tx2's ActiveTxns
	err := tx2.Set([]byte("key1"), []byte("val1-3"))
	if err == nil {
		t.Error("tx2 Set key1 should have failed with write conflict")
	} else if !dberror.IsWriteConflict(err) {
		t.Errorf("tx2 Set key1 error = %v, want WriteConflict", err)
	}

	// tx3 starts, adds new key and commits
	// tx3 is NOT in tx1's ActiveTxns (tx1 started before tx3)
	tx3 := mvcc.Begin()
	tx3.Set([]byte("key5"), []byte("val6"))
	tx3.Commit()

	// tx1 tries to modify key5
	// With the fixed implementation, this SHOULD fail with write conflict
	// because tx3 committed a version of key5 that is NOT visible to tx1
	// (tx3.version > tx1.version)
	err = tx1.Set([]byte("key5"), []byte("val6-1"))
	if err == nil {
		t.Error("tx1 Set key5 should have failed with write conflict (tx3 committed newer version)")
	} else if !dberror.IsWriteConflict(err) {
		t.Errorf("tx1 Set key5 error = %v, want WriteConflict", err)
	}

	tx1.Commit()
}

func TestWriteConflictDetection_MemoryEngine(t *testing.T) {
	testWriteConflictDetection(t, NewMemoryEngine())
}

func TestWriteConflictDetection_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testWriteConflictDetection(t, engine)
}

// ============================================================================
// 7. Delete Conflict Detection
// ============================================================================

func testDeleteConflictDetection(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Commit()

	// tx1 starts first
	tx1 := mvcc.Begin()

	// tx2 starts after tx1 (tx1 in tx2's ActiveTxns)
	tx2 := mvcc.Begin()

	// tx1 deletes key1 and modifies key2
	if err := tx1.Delete([]byte("key1")); err != nil {
		t.Fatalf("tx1 Delete key1 failed: %v", err)
	}
	if err := tx1.Set([]byte("key2"), []byte("val2-1")); err != nil {
		t.Fatalf("tx1 Set key2 failed: %v", err)
	}

	// tx2 tries to delete same keys - should get conflict
	// because tx1 is in tx2's ActiveTxns
	err := tx2.Delete([]byte("key1"))
	if err == nil {
		t.Error("tx2 Delete key1 should have failed with write conflict")
	} else if !dberror.IsWriteConflict(err) {
		t.Errorf("tx2 Delete key1 error = %v, want WriteConflict", err)
	}

	err = tx2.Delete([]byte("key2"))
	if err == nil {
		t.Error("tx2 Delete key2 should have failed with write conflict")
	} else if !dberror.IsWriteConflict(err) {
		t.Errorf("tx2 Delete key2 error = %v, want WriteConflict", err)
	}
}

func TestDeleteConflictDetection_MemoryEngine(t *testing.T) {
	testDeleteConflictDetection(t, NewMemoryEngine())
}

func TestDeleteConflictDetection_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testDeleteConflictDetection(t, engine)
}

// ============================================================================
// 8. Rollback: uncommitted changes discarded
// ============================================================================

func testRollback(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key3"), []byte("val3"))
	tx.Commit()

	// tx1 modifies keys but rolls back
	tx1 := mvcc.Begin()
	tx1.Set([]byte("key1"), []byte("val1-1"))
	tx1.Set([]byte("key2"), []byte("val2-1"))
	tx1.Set([]byte("key3"), []byte("val3-1"))
	if err := tx1.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// tx2 should see original values
	tx2 := mvcc.Begin()

	val, err := tx2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("key1 = %s, want val1 (rollback verified)", val)
	}

	val, err = tx2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val2")) {
		t.Errorf("key2 = %s, want val2 (rollback verified)", val)
	}

	val, err = tx2.Get([]byte("key3"))
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val3")) {
		t.Errorf("key3 = %s, want val3 (rollback verified)", val)
	}
}

func TestRollback_MemoryEngine(t *testing.T) {
	testRollback(t, NewMemoryEngine())
}

func TestRollback_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testRollback(t, engine)
}

// ============================================================================
// 9. MvccKey Encoding/Decoding
// ============================================================================

func TestMvccKeyEncodingDecoding(t *testing.T) {
	tests := []struct {
		name string
		key  *MvccKey
	}{
		{
			name: "NextVersion",
			key:  &MvccKey{keyType: mvccKeyNextVersion},
		},
		{
			name: "TxnActive",
			key:  &MvccKey{keyType: mvccKeyTxnActive, version: 100},
		},
		{
			name: "TxnWrite",
			key:  &MvccKey{keyType: mvccKeyTxnWrite, version: 100, data: []byte("testkey")},
		},
		{
			name: "Version",
			key:  &MvccKey{keyType: mvccKeyVersion, data: []byte("userkey"), version: 200},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.key.Encode()
			decoded := DecodeMvccKey(encoded)

			if decoded.keyType != tt.key.keyType {
				t.Errorf("keyType mismatch: got %d, want %d", decoded.keyType, tt.key.keyType)
			}

			if decoded.version != tt.key.version {
				t.Errorf("version mismatch: got %d, want %d", decoded.version, tt.key.version)
			}

			if !bytes.Equal(decoded.data, tt.key.data) {
				t.Errorf("data mismatch: got %v, want %v", decoded.data, tt.key.data)
			}
		})
	}
}

func TestMvccKeyHelperFunctions(t *testing.T) {
	// Test NextVersionKey
	nextVerKey := NextVersionKey()
	decoded := DecodeMvccKey(nextVerKey)
	if decoded.keyType != mvccKeyNextVersion {
		t.Errorf("NextVersionKey: wrong type %d", decoded.keyType)
	}

	// Test TxnActiveKey
	txnActiveKey := TxnActiveKey(42)
	decoded = DecodeMvccKey(txnActiveKey)
	if decoded.keyType != mvccKeyTxnActive {
		t.Errorf("TxnActiveKey: wrong type %d", decoded.keyType)
	}
	if decoded.version != 42 {
		t.Errorf("TxnActiveKey: wrong version %d", decoded.version)
	}

	// Test TxnWriteKey
	txnWriteKey := TxnWriteKey(100, []byte("mykey"))
	decoded = DecodeMvccKey(txnWriteKey)
	if decoded.keyType != mvccKeyTxnWrite {
		t.Errorf("TxnWriteKey: wrong type %d", decoded.keyType)
	}
	if decoded.version != 100 {
		t.Errorf("TxnWriteKey: wrong version %d", decoded.version)
	}
	if !bytes.Equal(decoded.data, []byte("mykey")) {
		t.Errorf("TxnWriteKey: wrong data %v", decoded.data)
	}

	// Test VersionKey
	versionKey := VersionKey([]byte("userkey"), 200)
	decoded = DecodeMvccKey(versionKey)
	if decoded.keyType != mvccKeyVersion {
		t.Errorf("VersionKey: wrong type %d", decoded.keyType)
	}
	if !bytes.Equal(decoded.data, []byte("userkey")) {
		t.Errorf("VersionKey: wrong data %v", decoded.data)
	}
	if decoded.version != 200 {
		t.Errorf("VersionKey: wrong version %d", decoded.version)
	}
}

// ============================================================================
// 10. Scan with MVCC Visibility
// ============================================================================

func testScanWithMvccVisibility(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("aabb"), []byte("val1"))
	tx.Set([]byte("abcc"), []byte("val2"))
	tx.Set([]byte("bbaa"), []byte("val3"))
	tx.Set([]byte("acca"), []byte("val4"))
	tx.Set([]byte("aaca"), []byte("val5"))
	tx.Set([]byte("bcca"), []byte("val6"))
	tx.Commit()

	// tx2 starts first and will make uncommitted changes
	tx2 := mvcc.Begin()

	// tx1 starts AFTER tx2, so tx2 is in tx1's ActiveTxns
	tx1 := mvcc.Begin()

	// tx2 makes uncommitted changes
	tx2.Set([]byte("acca"), []byte("val4-1"))
	tx2.Set([]byte("aabb"), []byte("val1-1"))

	// tx3 starts AFTER tx1, modifies and commits
	// tx3 is NOT in tx1's ActiveTxns (tx1 started before tx3)
	// So tx1 WILL see tx3's committed changes
	tx3 := mvcc.Begin()
	tx3.Set([]byte("bbaa"), []byte("val3-1"))
	tx3.Delete([]byte("bcca"))
	tx3.Commit()

	// tx1 scans - visibility rules:
	// - tx2's changes NOT visible (tx2 in tx1's ActiveTxns)
	// - tx3's changes also NOT visible because:
	//   The Scan function returns the FIRST visible version for each key.
	//   Since versions are ordered ascending, original versions (v1) come before
	//   tx3's versions (v4). Original versions ARE visible (committed, not in ActiveTxns),
	//   so they are returned instead of tx3's newer versions.
	iter := tx1.Scan([]byte{0x03}, []byte{0x04})
	results := collectScanResults(iter)

	// tx1 should see (first visible version for each key):
	// - aabb(val1) - tx2's change hidden, original visible first
	// - aaca(val5) - unchanged
	// - abcc(val2) - unchanged
	// - acca(val4) - tx2's change hidden, original visible first
	// - bbaa(val3) - original visible first (tx3's val3-1 comes later in iteration)
	// - bcca(val6) - original visible first (tx3's tombstone comes later in iteration)
	// Total: 6 visible keys, all with original values
	expectedCount := 6
	if len(results) != expectedCount {
		t.Errorf("Scan: got %d results, want %d", len(results), expectedCount)
		for i, r := range results {
			t.Logf("Result %d: key=%s, value=%s", i, r.Key, r.Value)
		}
	}

	// Verify tx2's uncommitted changes are not visible
	for _, r := range results {
		if bytes.Equal(r.Key, []byte("aabb")) && !bytes.Equal(r.Value, []byte("val1")) {
			t.Errorf("aabb = %s, want val1 (tx2 uncommitted)", r.Value)
		}
		if bytes.Equal(r.Key, []byte("acca")) && !bytes.Equal(r.Value, []byte("val4")) {
			t.Errorf("acca = %s, want val4 (tx2 uncommitted)", r.Value)
		}
	}

	// Verify tx3's changes are also not visible (first visible version is original)
	for _, r := range results {
		if bytes.Equal(r.Key, []byte("bbaa")) {
			if !bytes.Equal(r.Value, []byte("val3")) {
				t.Errorf("bbaa = %s, want val3 (first visible version)", r.Value)
			}
		}
	}

	// Verify bcca IS in results with original value (tx3's tombstone comes later in iteration)
	bccaFound := false
	for _, r := range results {
		if bytes.Equal(r.Key, []byte("bcca")) {
			bccaFound = true
			if !bytes.Equal(r.Value, []byte("val6")) {
				t.Errorf("bcca = %s, want val6 (first visible version)", r.Value)
			}
		}
	}
	if !bccaFound {
		t.Errorf("bcca should be in results with original value")
	}
}

func TestScanWithMvccVisibility_MemoryEngine(t *testing.T) {
	testScanWithMvccVisibility(t, NewMemoryEngine())
}

func TestScanWithMvccVisibility_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testScanWithMvccVisibility(t, engine)
}

// ============================================================================
// Additional Tests: Set Operations
// ============================================================================

func testSetOperations(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key2"), []byte("val3"))
	tx.Set([]byte("key3"), []byte("val4"))
	tx.Set([]byte("key4"), []byte("val5"))
	tx.Commit()

	// tx1 and tx2 start concurrently (each in other's ActiveTxns)
	tx1 := mvcc.Begin()
	tx2 := mvcc.Begin()

	// tx1 modifies key1 and key2
	tx1.Set([]byte("key1"), []byte("val1-1"))
	tx1.Set([]byte("key2"), []byte("val3-1"))
	tx1.Set([]byte("key2"), []byte("val3-2"))

	// tx2 modifies different keys (key3, key4) - no conflict
	tx2.Set([]byte("key3"), []byte("val4-1"))
	tx2.Set([]byte("key4"), []byte("val5-1"))

	// Both commit
	tx1.Commit()
	tx2.Commit()

	// New transaction should see all changes
	tx = mvcc.Begin()

	val, err := tx.Get([]byte("key1"))
	if err != nil || !bytes.Equal(val, []byte("val1-1")) {
		t.Errorf("key1 = %s, want val1-1", val)
	}

	val, err = tx.Get([]byte("key2"))
	if err != nil || !bytes.Equal(val, []byte("val3-2")) {
		t.Errorf("key2 = %s, want val3-2", val)
	}

	val, err = tx.Get([]byte("key3"))
	if err != nil || !bytes.Equal(val, []byte("val4-1")) {
		t.Errorf("key3 = %s, want val4-1", val)
	}

	val, err = tx.Get([]byte("key4"))
	if err != nil || !bytes.Equal(val, []byte("val5-1")) {
		t.Errorf("key4 = %s, want val5-1", val)
	}
}

func TestSetOperations_MemoryEngine(t *testing.T) {
	testSetOperations(t, NewMemoryEngine())
}

func TestSetOperations_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testSetOperations(t, engine)
}

// ============================================================================
// Additional Tests: Delete Operations
// ============================================================================

func testDeleteOperations(t *testing.T, engine Engine) {
	mvcc := NewMvcc(engine)

	// Setup initial data with deletes in same transaction
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Set([]byte("key2"), []byte("val2"))
	tx.Set([]byte("key3"), []byte("val3"))
	tx.Delete([]byte("key2"))
	tx.Delete([]byte("key3"))
	tx.Set([]byte("key3"), []byte("val3-1"))
	tx.Commit()

	// Verify key2 is deleted (tombstone)
	tx1 := mvcc.Begin()
	val, err := tx1.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !isTombstone(val) {
		t.Errorf("key2 = %s, want nil/empty (deleted)", val)
	}

	// Verify scan shows correct keys (excluding tombstones)
	// Scan uses raw engine keys starting with 0x03
	iter := tx1.Scan([]byte{0x03}, []byte{0x04})
	results := collectScanResults(iter)

	// Should have key1 and key3 (key2 is tombstone)
	expected := []ScanResult{
		{Key: []byte("key1"), Value: []byte("val1")},
		{Key: []byte("key3"), Value: []byte("val3-1")},
	}

	if len(results) != len(expected) {
		t.Errorf("Scan: got %d results, want %d", len(results), len(expected))
		for i, r := range results {
			t.Logf("Result %d: key=%s, value=%s", i, r.Key, r.Value)
		}
		return
	}

	for i, exp := range expected {
		if !bytes.Equal(results[i].Key, exp.Key) {
			t.Errorf("Result %d key: got %s, want %s", i, results[i].Key, exp.Key)
		}
		if !bytes.Equal(results[i].Value, exp.Value) {
			t.Errorf("Result %d value: got %s, want %s", i, results[i].Value, exp.Value)
		}
	}
}

func TestDeleteOperations_MemoryEngine(t *testing.T) {
	testDeleteOperations(t, NewMemoryEngine())
}

func TestDeleteOperations_DiskEngine(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}
	defer engine.Close()
	testDeleteOperations(t, engine)
}

// ============================================================================
// Transaction Version Tests
// ============================================================================

func TestTransactionVersions(t *testing.T) {
	mvcc := NewMvcc(NewMemoryEngine())

	tx1 := mvcc.Begin()
	tx2 := mvcc.Begin()
	tx3 := mvcc.Begin()

	// Each transaction should have unique version
	versions := make(map[uint64]bool)
	versions[tx1.Version()] = true
	versions[tx2.Version()] = true
	versions[tx3.Version()] = true

	if len(versions) != 3 {
		t.Errorf("Expected 3 unique versions, got %d", len(versions))
	}

	// Versions should be increasing
	if tx2.Version() <= tx1.Version() {
		t.Errorf("tx2 version %d should be > tx1 version %d", tx2.Version(), tx1.Version())
	}
	if tx3.Version() <= tx2.Version() {
		t.Errorf("tx3 version %d should be > tx2 version %d", tx3.Version(), tx2.Version())
	}
}

// ============================================================================
// Own Writes Visibility Test
// ============================================================================

func TestOwnWritesVisibility(t *testing.T) {
	mvcc := NewMvcc(NewMemoryEngine())

	tx := mvcc.Begin()

	// Write and read in same transaction
	tx.Set([]byte("key1"), []byte("val1"))

	val, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Own write not visible: got %s, want val1", val)
	}

	// Update and read again
	tx.Set([]byte("key1"), []byte("val2"))

	val, err = tx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val2")) {
		t.Errorf("Own update not visible: got %s, want val2", val)
	}

	tx.Commit()
}

// ============================================================================
// Cleanup test for DiskEngine
// ============================================================================

func TestDiskEngineCleanup(t *testing.T) {
	// Create a temporary directory outside of t.TempDir() to test manual cleanup
	dir := filepath.Join(os.TempDir(), "godb-mvcc-test-cleanup")
	defer os.RemoveAll(dir)

	engine, err := NewDiskEngine(filepath.Join(dir, "sqldb-log"))
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}

	mvcc := NewMvcc(engine)
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("val1"))
	tx.Commit()

	// Verify data persists
	tx2 := mvcc.Begin()
	val, err := tx2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("key1 = %s, want val1", val)
	}

	engine.Close()
}

// ============================================================================
// Version Counter Persistence Test
// ============================================================================

func TestVersionCounterPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create engine and run some transactions
	engine1, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create disk engine: %v", err)
	}

	mvcc1 := NewMvcc(engine1)
	tx1 := mvcc1.Begin()
	tx1.Set([]byte("key1"), []byte("val1"))
	tx1.Commit()

	versionAfterTx1 := mvcc1.version.Load()

	// Verify version counter is persisted to storage
	// The NextVersion key should contain versionAfterTx1 + 1
	nextVersionKey := NextVersionKey()
	val, err := engine1.Get(nextVersionKey)
	if err != nil {
		t.Fatalf("Failed to get NextVersion key: %v", err)
	}
	if val == nil {
		t.Fatal("NextVersion key not found in storage")
	}
	if len(val) != 8 {
		t.Fatalf("NextVersion value wrong length: got %d, want 8", len(val))
	}

	persistedVersion := binary.BigEndian.Uint64(val)
	expectedVersion := versionAfterTx1 + 1
	if persistedVersion != expectedVersion {
		t.Errorf("Persisted version = %d, want %d", persistedVersion, expectedVersion)
	}

	// Create more transactions and verify version continues to increment
	tx2 := mvcc1.Begin()
	tx2.Set([]byte("key2"), []byte("val2"))
	tx2.Commit()

	versionAfterTx2 := mvcc1.version.Load()
	if versionAfterTx2 <= versionAfterTx1 {
		t.Errorf("Version should increment: got %d, want > %d", versionAfterTx2, versionAfterTx1)
	}

	// Verify the persisted version was updated
	val, err = engine1.Get(nextVersionKey)
	if err != nil {
		t.Fatalf("Failed to get NextVersion key after tx2: %v", err)
	}
	persistedVersion = binary.BigEndian.Uint64(val)
	expectedVersion = versionAfterTx2 + 1
	if persistedVersion != expectedVersion {
		t.Errorf("Persisted version after tx2 = %d, want %d", persistedVersion, expectedVersion)
	}

	engine1.Close()
}
