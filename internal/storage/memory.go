package storage

import (
	"bytes"
	"sort"
	"sync"
)

// MemoryEngine is an in-memory storage engine using a sorted map
type MemoryEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
	keys [][]byte // sorted keys
}

// NewMemoryEngine creates a new memory engine
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		data: make(map[string][]byte),
		keys: make([][]byte, 0),
	}
}

// Set sets a key-value pair
func (e *MemoryEngine) Set(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	keyStr := string(key)
	if _, exists := e.data[keyStr]; !exists {
		// Insert key in sorted order
		idx := sort.Search(len(e.keys), func(i int) bool {
			return bytes.Compare(e.keys[i], key) >= 0
		})
		e.keys = append(e.keys, nil)
		copy(e.keys[idx+1:], e.keys[idx:])
		e.keys[idx] = key
	}
	e.data[keyStr] = value
	return nil
}

// Get gets a value by key
func (e *MemoryEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if v, ok := e.data[string(key)]; ok {
		return v, nil
	}
	return nil, nil
}

// Delete deletes a key
func (e *MemoryEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	keyStr := string(key)
	if _, exists := e.data[keyStr]; exists {
		delete(e.data, keyStr)
		// Remove from sorted keys
		idx := sort.Search(len(e.keys), func(i int) bool {
			return bytes.Compare(e.keys[i], key) >= 0
		})
		if idx < len(e.keys) && bytes.Equal(e.keys[idx], key) {
			e.keys = append(e.keys[:idx], e.keys[idx+1:]...)
		}
	}
	return nil
}

// Scan scans a range of keys [start, end)
func (e *MemoryEngine) Scan(start, end []byte) EngineIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys [][]byte
	var values [][]byte
	for _, k := range e.keys {
		if (start == nil || bytes.Compare(k, start) >= 0) &&
			(end == nil || bytes.Compare(k, end) < 0) {
			keys = append(keys, k)
			values = append(values, e.data[string(k)])
		}
	}

	return &MemoryIterator{
		keys:   keys,
		values: values,
		pos:    -1,
	}
}

// ScanPrefix scans all keys with a given prefix
func (e *MemoryEngine) ScanPrefix(prefix []byte) EngineIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys [][]byte
	var values [][]byte
	for _, k := range e.keys {
		if bytes.HasPrefix(k, prefix) {
			keys = append(keys, k)
			values = append(values, e.data[string(k)])
		}
	}

	return &MemoryIterator{
		keys:   keys,
		values: values,
		pos:    -1,
	}
}

// MemoryIterator is an iterator over memory engine
type MemoryIterator struct {
	keys   [][]byte
	values [][]byte
	pos    int
	err    error
}

// Next advances the iterator
func (it *MemoryIterator) Next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

// Key returns the current key
func (it *MemoryIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return it.keys[it.pos]
}

// Value returns the current value
func (it *MemoryIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.values) {
		return nil
	}
	return it.values[it.pos]
}

// Err returns any error encountered
func (it *MemoryIterator) Err() error {
	return it.err
}

// Close closes the iterator
func (it *MemoryIterator) Close() {}
