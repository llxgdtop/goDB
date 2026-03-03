package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskEngine_SetAndGet(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Test basic set and get
	key := []byte("testkey")
	value := []byte("testvalue")

	err = engine.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned wrong value: got %s, want %s", got, value)
	}
}

func TestDiskEngine_GetNonExistent(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Get a key that doesn't exist
	got, err := engine.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got != nil {
		t.Errorf("Get should return nil for non-existent key, got %v", got)
	}
}

func TestDiskEngine_Delete(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	key := []byte("deletekey")
	value := []byte("deletevalue")

	// Set then delete
	err = engine.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = engine.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	got, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got != nil {
		t.Errorf("Get should return nil after delete, got %v", got)
	}
}

func TestDiskEngine_DeleteNonExistent(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Deleting a non-existent key should not error
	err = engine.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Delete of non-existent key failed: %v", err)
	}
}

func TestDiskEngine_Overwrite(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	key := []byte("overwritekey")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Set initial value
	err = engine.Set(key, value1)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Overwrite with new value
	err = engine.Set(key, value2)
	if err != nil {
		t.Fatalf("Set (overwrite) failed: %v", err)
	}

	got, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value2) {
		t.Errorf("Get returned wrong value after overwrite: got %s, want %s", got, value2)
	}
}

func TestDiskEngine_EmptyKeyAndValue(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Test empty key
	emptyKey := []byte{}
	value := []byte("value")

	err = engine.Set(emptyKey, value)
	if err != nil {
		t.Fatalf("Set with empty key failed: %v", err)
	}

	got, err := engine.Get(emptyKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned wrong value for empty key: got %s, want %s", got, value)
	}

	// Test empty value
	key := []byte("key")
	emptyValue := []byte{}

	err = engine.Set(key, emptyValue)
	if err != nil {
		t.Fatalf("Set with empty value failed: %v", err)
	}

	got, err = engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, emptyValue) {
		t.Errorf("Get returned wrong value for empty value: got %s, want empty", got)
	}
}

func TestDiskEngine_Scan(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Insert multiple keys
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	values := [][]byte{[]byte("va"), []byte("vb"), []byte("vc"), []byte("vd"), []byte("ve")}

	for i, key := range keys {
		err := engine.Set(key, values[i])
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Scan all keys
	t.Run("ScanAll", func(t *testing.T) {
		iter := engine.Scan(nil, nil)
		defer iter.Close()

		var resultKeys [][]byte
		var resultValues [][]byte
		for iter.Next() {
			resultKeys = append(resultKeys, iter.Key())
			resultValues = append(resultValues, iter.Value())
		}

		if iter.Err() != nil {
			t.Fatalf("Iterator error: %v", iter.Err())
		}

		if len(resultKeys) != len(keys) {
			t.Errorf("Scan returned wrong number of keys: got %d, want %d", len(resultKeys), len(keys))
		}

		// Verify sorted order
		for i, k := range resultKeys {
			if !bytes.Equal(k, keys[i]) {
				t.Errorf("Key at position %d: got %s, want %s", i, k, keys[i])
			}
			if !bytes.Equal(resultValues[i], values[i]) {
				t.Errorf("Value at position %d: got %s, want %s", i, resultValues[i], values[i])
			}
		}
	})

	// Scan with start and end
	t.Run("ScanRange", func(t *testing.T) {
		iter := engine.Scan([]byte("b"), []byte("d"))
		defer iter.Close()

		var resultKeys [][]byte
		for iter.Next() {
			resultKeys = append(resultKeys, iter.Key())
		}

		if iter.Err() != nil {
			t.Fatalf("Iterator error: %v", iter.Err())
		}

		// Should return "b" and "c" (range is [start, end))
		expected := [][]byte{[]byte("b"), []byte("c")}
		if len(resultKeys) != len(expected) {
			t.Errorf("Scan range returned wrong number of keys: got %d, want %d", len(resultKeys), len(expected))
		}

		for i, k := range resultKeys {
			if !bytes.Equal(k, expected[i]) {
				t.Errorf("Key at position %d: got %s, want %s", i, k, expected[i])
			}
		}
	})

	// Scan with only start
	t.Run("ScanFromStart", func(t *testing.T) {
		iter := engine.Scan([]byte("c"), nil)
		defer iter.Close()

		var resultKeys [][]byte
		for iter.Next() {
			resultKeys = append(resultKeys, iter.Key())
		}

		if iter.Err() != nil {
			t.Fatalf("Iterator error: %v", iter.Err())
		}

		// Should return "c", "d", "e"
		expected := [][]byte{[]byte("c"), []byte("d"), []byte("e")}
		if len(resultKeys) != len(expected) {
			t.Errorf("Scan from start returned wrong number of keys: got %d, want %d", len(resultKeys), len(expected))
		}
	})

	// Scan with only end
	t.Run("ScanToEnd", func(t *testing.T) {
		iter := engine.Scan(nil, []byte("c"))
		defer iter.Close()

		var resultKeys [][]byte
		for iter.Next() {
			resultKeys = append(resultKeys, iter.Key())
		}

		if iter.Err() != nil {
			t.Fatalf("Iterator error: %v", iter.Err())
		}

		// Should return "a", "b"
		expected := [][]byte{[]byte("a"), []byte("b")}
		if len(resultKeys) != len(expected) {
			t.Errorf("Scan to end returned wrong number of keys: got %d, want %d", len(resultKeys), len(expected))
		}
	})

	// Scan empty range
	t.Run("ScanEmptyRange", func(t *testing.T) {
		iter := engine.Scan([]byte("x"), []byte("z"))
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != 0 {
			t.Errorf("Scan of empty range should return 0 keys, got %d", count)
		}
	})
}

func TestDiskEngine_ScanPrefix(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Insert keys with various prefixes
	testData := map[string]string{
		"prefix_a": "value1",
		"prefix_b": "value2",
		"prefix_c": "value3",
		"other_a":  "value4",
		"other_b":  "value5",
		"no_prefix": "value6",
	}

	for k, v := range testData {
		err := engine.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Scan with prefix "prefix_"
	iter := engine.ScanPrefix([]byte("prefix_"))
	defer iter.Close()

	var resultKeys [][]byte
	var resultValues [][]byte
	for iter.Next() {
		resultKeys = append(resultKeys, iter.Key())
		resultValues = append(resultValues, iter.Value())
	}

	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}

	// Should return 3 keys with "prefix_" prefix
	if len(resultKeys) != 3 {
		t.Errorf("ScanPrefix returned wrong number of keys: got %d, want 3", len(resultKeys))
	}

	// Verify all returned keys have the prefix
	for _, k := range resultKeys {
		if !bytes.HasPrefix(k, []byte("prefix_")) {
			t.Errorf("ScanPrefix returned key without prefix: %s", k)
		}
	}

	// Verify sorted order
	expectedKeys := [][]byte{[]byte("prefix_a"), []byte("prefix_b"), []byte("prefix_c")}
	for i, k := range resultKeys {
		if !bytes.Equal(k, expectedKeys[i]) {
			t.Errorf("Key at position %d: got %s, want %s", i, k, expectedKeys[i])
		}
	}

	// Test empty prefix (should return all keys)
	t.Run("EmptyPrefix", func(t *testing.T) {
		iter := engine.ScanPrefix([]byte{})
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != len(testData) {
			t.Errorf("ScanPrefix with empty prefix returned wrong count: got %d, want %d", count, len(testData))
		}
	})

	// Test non-matching prefix
	t.Run("NonMatchingPrefix", func(t *testing.T) {
		iter := engine.ScanPrefix([]byte("xyz_"))
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != 0 {
			t.Errorf("ScanPrefix with non-matching prefix should return 0 keys, got %d", count)
		}
	})
}

func TestDiskEngine_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Write data
	engine1, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := engine1.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Close the engine
	err = engine1.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify data persists
	engine2, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine (reopen) failed: %v", err)
	}
	defer engine2.Close()

	for k, v := range testData {
		got, err := engine2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Errorf("Get returned wrong value for key %s: got %s, want %s", k, got, v)
		}
	}
}

func TestDiskEngine_PersistenceWithDelete(t *testing.T) {
	dir := t.TempDir()

	// Write data
	engine1, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}

	err = engine1.Set([]byte("keep"), []byte("value1"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = engine1.Set([]byte("delete"), []byte("value2"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete one key
	err = engine1.Delete([]byte("delete"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Close the engine
	err = engine1.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify deleted key is still deleted
	engine2, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine (reopen) failed: %v", err)
	}
	defer engine2.Close()

	// Verify kept key exists
	got, err := engine2.Get([]byte("keep"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("value1")) {
		t.Errorf("Get returned wrong value: got %s, want value1", got)
	}

	// Verify deleted key is gone
	got, err = engine2.Get([]byte("delete"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != nil {
		t.Errorf("Deleted key should not exist, got %v", got)
	}
}

func TestDiskEngine_PersistenceWithOverwrite(t *testing.T) {
	dir := t.TempDir()

	// Write data
	engine1, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}

	// Set initial value
	err = engine1.Set([]byte("key"), []byte("original"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Overwrite
	err = engine1.Set([]byte("key"), []byte("updated"))
	if err != nil {
		t.Fatalf("Set (overwrite) failed: %v", err)
	}

	// Close the engine
	err = engine1.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify updated value persists
	engine2, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine (reopen) failed: %v", err)
	}
	defer engine2.Close()

	got, err := engine2.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("updated")) {
		t.Errorf("Get returned wrong value: got %s, want updated", got)
	}
}

func TestDiskEngine_Compact(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Write data
	engine.Set([]byte("key1"), []byte("value1"))
	engine.Set([]byte("key2"), []byte("value2"))
	engine.Set([]byte("key3"), []byte("value3"))

	// Delete some keys
	engine.Delete([]byte("key1"))
	engine.Delete([]byte("key2"))

	// Overwrite a key multiple times
	engine.Set([]byte("aa"), []byte("value1"))
	engine.Set([]byte("aa"), []byte("value2"))
	engine.Set([]byte("aa"), []byte("value3"))
	engine.Set([]byte("bb"), []byte("value4"))
	engine.Set([]byte("bb"), []byte("value5"))

	// Verify before compact
	iter := engine.Scan(nil, nil)
	var beforeCompact [][]byte
	for iter.Next() {
		beforeCompact = append(beforeCompact, iter.Key())
	}
	iter.Close()

	// Compact
	err = engine.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify data after compact
	iter = engine.Scan(nil, nil)
	var afterCompactKeys [][]byte
	var afterCompactValues [][]byte
	for iter.Next() {
		afterCompactKeys = append(afterCompactKeys, iter.Key())
		afterCompactValues = append(afterCompactValues, iter.Value())
	}
	iter.Close()

	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}

	// Verify expected data
	expected := []struct {
		key   string
		value string
	}{
		{"aa", "value3"},
		{"bb", "value5"},
		{"key3", "value3"},
	}

	if len(afterCompactKeys) != len(expected) {
		t.Errorf("After compact, wrong number of keys: got %d, want %d", len(afterCompactKeys), len(expected))
	}

	for i, exp := range expected {
		if i >= len(afterCompactKeys) {
			break
		}
		if string(afterCompactKeys[i]) != exp.key {
			t.Errorf("Key at position %d: got %s, want %s", i, afterCompactKeys[i], exp.key)
		}
		if string(afterCompactValues[i]) != exp.value {
			t.Errorf("Value at position %d: got %s, want %s", i, afterCompactValues[i], exp.value)
		}
	}
}

func TestDiskEngine_CompactAndPersist(t *testing.T) {
	dir := t.TempDir()

	// Write data and compact
	engine1, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}

	engine1.Set([]byte("key1"), []byte("value1"))
	engine1.Set([]byte("key2"), []byte("value2"))
	engine1.Delete([]byte("key1"))

	err = engine1.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	err = engine1.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	engine2, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine (reopen) failed: %v", err)
	}
	defer engine2.Close()

	// key1 should be deleted
	got, err := engine2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != nil {
		t.Errorf("Deleted key should not exist after compact and reopen")
	}

	// key2 should exist
	got, err = engine2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("value2")) {
		t.Errorf("Get returned wrong value: got %s, want value2", got)
	}
}

func TestDiskEngine_MultipleOperations(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}
	defer engine.Close()

	// Test multiple operations in sequence
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		err := engine.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed at iteration %d: %v", i, err)
		}
	}

	// Verify all values
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expected := []byte{byte(i * 2)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get failed at iteration %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("Get at iteration %d: got %v, want %v", i, got, expected)
		}
	}

	// Delete half the keys
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		err := engine.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed at iteration %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get failed at iteration %d: %v", i, err)
		}
		if got != nil {
			t.Errorf("Get after delete at iteration %d: expected nil, got %v", i, got)
		}
	}

	// Verify remaining keys still exist
	for i := 50; i < 100; i++ {
		key := []byte{byte(i)}
		expected := []byte{byte(i * 2)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get failed at iteration %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("Get at iteration %d: got %v, want %v", i, got, expected)
		}
	}
}

func TestDiskEngine_DataFileExists(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewDiskEngine(dir)
	if err != nil {
		t.Fatalf("NewDiskEngine failed: %v", err)
	}

	// Verify data file was created
	dataPath := filepath.Join(dir, "data.log")
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Error("Data file was not created")
	}

	engine.Close()
}
