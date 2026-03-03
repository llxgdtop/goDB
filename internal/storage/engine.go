package storage

// EngineIterator is an iterator over key-value pairs
type EngineIterator interface {
	// Next advances the iterator
	Next() bool
	// Key returns the current key
	Key() []byte
	// Value returns the current value
	Value() []byte
	// Err returns any error encountered
	Err() error
	// Close closes the iterator
	Close()
}

// Engine is the storage engine interface
type Engine interface {
	// Set sets a key-value pair
	Set(key, value []byte) error
	// Get gets a value by key
	Get(key []byte) ([]byte, error)
	// Delete deletes a key
	Delete(key []byte) error
	// Scan scans a range of keys
	Scan(start, end []byte) EngineIterator
	// ScanPrefix scans all keys with a given prefix
	ScanPrefix(prefix []byte) EngineIterator
}
