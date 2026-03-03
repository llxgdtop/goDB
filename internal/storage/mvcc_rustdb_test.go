package storage

import (
	"bytes"
	"testing"
)

// TestRustDBVisibilityRules tests that the MVCC implementation matches RustDB's visibility rules:
// 1. If a version is in the active transaction set, it's NOT visible (dirty read prevention)
// 2. Otherwise, it's visible if version <= transaction.version (snapshot isolation)
func TestRustDBVisibilityRules(t *testing.T) {
	engine := NewMemoryEngine()
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("initial"))
	tx.Commit()

	// tx1 starts first (version will be higher than initial tx)
	tx1 := mvcc.Begin()

	// tx2 starts AFTER tx1, so tx1 is in tx2's active set
	tx2 := mvcc.Begin()
	tx2.Set([]byte("key1"), []byte("tx2_uncommitted"))

	// tx1 should NOT see tx2's uncommitted changes
	// because tx2 started AFTER tx1, so tx2's version > tx1.version
	val, err := tx1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("tx1 should see initial value, got %s", val)
	}

	// Now test the reverse: tx3 starts first, tx1 starts after
	tx3 := mvcc.Begin()
	tx3.Set([]byte("key2"), []byte("tx3_uncommitted"))

	// tx4 starts AFTER tx3, so tx3 is in tx4's active set
	tx4 := mvcc.Begin()

	// tx4 should NOT see tx3's uncommitted changes
	// because tx3's version is in tx4's active set
	val, err = tx4.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("tx4 should not see tx3's uncommitted changes, got %s", val)
	}

	// Commit tx3
	tx3.Commit()

	// tx4 still should NOT see tx3's changes due to snapshot isolation
	// tx3 was active when tx4 started, so it's in tx4's active snapshot
	val, err = tx4.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("tx4 should not see tx3's changes due to snapshot isolation, got %s", val)
	}

	// But a new transaction should see tx3's committed changes
	tx5 := mvcc.Begin()
	val, err = tx5.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("tx3_uncommitted")) {
		t.Errorf("tx5 should see tx3's committed changes, got %s", val)
	}
}

// TestActiveTransactionIsolation tests that transactions don't see each other's changes
func TestActiveTransactionIsolation(t *testing.T) {
	engine := NewMemoryEngine()
	mvcc := NewMvcc(engine)

	// Setup initial data
	tx := mvcc.Begin()
	tx.Set([]byte("key1"), []byte("initial"))
	tx.Commit()

	// Start two concurrent transactions
	tx1 := mvcc.Begin()
	tx2 := mvcc.Begin()

	// Both transactions should see the initial state
	val, _ := tx1.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("tx1 should see initial value, got %s", val)
	}

	val, _ = tx2.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("tx2 should see initial value, got %s", val)
	}

	// Modify in tx1
	tx1.Set([]byte("key1"), []byte("tx1_value"))

	// tx2 should not see tx1's uncommitted changes
	val, _ = tx2.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("tx2 should still see initial value, got %s", val)
	}

	// Commit tx1
	tx1.Commit()

	// tx2 should still not see tx1's changes (snapshot isolation)
	val, _ = tx2.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("initial")) {
		t.Errorf("tx2 should still see initial value due to snapshot isolation, got %s", val)
	}

	// New transaction should see the committed value
	tx3 := mvcc.Begin()
	val, _ = tx3.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("tx1_value")) {
		t.Errorf("tx3 should see tx1's committed value, got %s", val)
	}
}