package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DiskEngine is a disk-based storage engine using Bitcask-style log-structured storage
type DiskEngine struct {
	mu     sync.RWMutex
	keydir map[string]keyDirEntry // key -> (offset, valueLen)
	log    *os.File
	dir    string
}

type keyDirEntry struct {
	offset   int64
	valueLen uint32
}

// NewDiskEngine creates a new disk engine
func NewDiskEngine(dir string) (*DiskEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(dir, "data.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	engine := &DiskEngine{
		keydir: make(map[string]keyDirEntry),
		log:    logFile,
		dir:    dir,
	}

	// Rebuild keydir from existing log
	if err := engine.rebuildKeyDir(); err != nil {
		return nil, err
	}

	return engine, nil
}

// rebuildKeyDir rebuilds the keydir from the log file
func (e *DiskEngine) rebuildKeyDir() error {
	var offset int64 = 0

	for {
		// Read key length (4 bytes)
		keyLenBuf := make([]byte, 4)
		_, err := e.log.ReadAt(keyLenBuf, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf)

		// Read value length (4 bytes)
		valLenBuf := make([]byte, 4)
		_, err = e.log.ReadAt(valLenBuf, offset+4)
		if err != nil {
			return err
		}
		valLen := binary.BigEndian.Uint32(valLenBuf)

		// Read key
		keyBuf := make([]byte, keyLen)
		_, err = e.log.ReadAt(keyBuf, offset+8)
		if err != nil {
			return err
		}

		// Check for tombstone (deleted)
		if valLen == 0xFFFFFFFF {
			delete(e.keydir, string(keyBuf))
		} else {
			e.keydir[string(keyBuf)] = keyDirEntry{
				offset:   offset,
				valueLen: valLen,
			}
		}

		// Move to next record
		offset += int64(8 + keyLen + valLen)
	}

	return nil
}

// Set sets a key-value pair
func (e *DiskEngine) Set(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Write record: keyLen(4) + valLen(4) + key + value
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(key)))
	binary.Write(&buf, binary.BigEndian, uint32(len(value)))
	buf.Write(key)
	buf.Write(value)

	offset, err := e.log.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err := e.log.Write(buf.Bytes()); err != nil {
		return err
	}

	e.keydir[string(key)] = keyDirEntry{
		offset:   offset,
		valueLen: uint32(len(value)),
	}

	return nil
}

// Get gets a value by key
func (e *DiskEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entry, ok := e.keydir[string(key)]
	if !ok {
		return nil, nil
	}

	// Read value length
	valLenBuf := make([]byte, 4)
	_, err := e.log.ReadAt(valLenBuf, entry.offset+4)
	if err != nil {
		return nil, err
	}
	valLen := binary.BigEndian.Uint32(valLenBuf)

	// Read key length
	keyLenBuf := make([]byte, 4)
	_, err = e.log.ReadAt(keyLenBuf, entry.offset)
	if err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// Read value
	value := make([]byte, valLen)
	_, err = e.log.ReadAt(value, entry.offset+8+int64(keyLen))
	if err != nil {
		return nil, err
	}

	return value, nil
}

// Delete deletes a key
func (e *DiskEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Write tombstone record
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(key)))
	binary.Write(&buf, binary.BigEndian, uint32(0xFFFFFFFF)) // Tombstone marker
	buf.Write(key)

	if _, err := e.log.Write(buf.Bytes()); err != nil {
		return err
	}

	delete(e.keydir, string(key))
	return nil
}

// Scan scans a range of keys [start, end)
func (e *DiskEngine) Scan(start, end []byte) EngineIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys [][]byte
	var values [][]byte
	for k, entry := range e.keydir {
		key := []byte(k)
		if (start == nil || bytes.Compare(key, start) >= 0) &&
			(end == nil || bytes.Compare(key, end) < 0) {
			keys = append(keys, key)
			value, err := e.getAt(entry)
			if err != nil {
				continue
			}
			values = append(values, value)
		}
	}

	// Sort keys and values together
	indices := make([]int, len(keys))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(keys[indices[i]], keys[indices[j]]) < 0
	})

	sortedKeys := make([][]byte, len(keys))
	sortedValues := make([][]byte, len(values))
	for i, idx := range indices {
		sortedKeys[i] = keys[idx]
		sortedValues[i] = values[idx]
	}

	return &DiskIterator{
		keys:   sortedKeys,
		values: sortedValues,
		pos:    -1,
	}
}

// ScanPrefix scans all keys with a given prefix
func (e *DiskEngine) ScanPrefix(prefix []byte) EngineIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var keys [][]byte
	var values [][]byte
	for k, entry := range e.keydir {
		key := []byte(k)
		if bytes.HasPrefix(key, prefix) {
			keys = append(keys, key)
			value, err := e.getAt(entry)
			if err != nil {
				continue
			}
			values = append(values, value)
		}
	}

	// Sort keys and values together
	indices := make([]int, len(keys))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(keys[indices[i]], keys[indices[j]]) < 0
	})

	sortedKeys := make([][]byte, len(keys))
	sortedValues := make([][]byte, len(values))
	for i, idx := range indices {
		sortedKeys[i] = keys[idx]
		sortedValues[i] = values[idx]
	}

	return &DiskIterator{
		keys:   sortedKeys,
		values: sortedValues,
		pos:    -1,
	}
}

// Close closes the disk engine
func (e *DiskEngine) Close() error {
	return e.log.Close()
}

// Compact compacts the log file by removing deleted entries
func (e *DiskEngine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Create new log file
	newPath := filepath.Join(e.dir, "data.log.new")
	newFile, err := os.Create(newPath)
	if err != nil {
		return err
	}

	// Write all current entries to new file
	newKeyDir := make(map[string]keyDirEntry)
	for k, entry := range e.keydir {
		value, err := e.getAt(entry)
		if err != nil {
			newFile.Close()
			return err
		}

		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint32(len(k)))
		binary.Write(&buf, binary.BigEndian, uint32(len(value)))
		buf.Write([]byte(k))
		buf.Write(value)

		offset, err := newFile.Seek(0, io.SeekEnd)
		if err != nil {
			newFile.Close()
			return err
		}

		if _, err := newFile.Write(buf.Bytes()); err != nil {
			newFile.Close()
			return err
		}

		newKeyDir[k] = keyDirEntry{
			offset:   offset,
			valueLen: uint32(len(value)),
		}
	}

	// Replace old file with new file
	if err := e.log.Close(); err != nil {
		newFile.Close()
		return err
	}

	oldPath := filepath.Join(e.dir, "data.log")
	if err := os.Rename(newPath, oldPath); err != nil {
		newFile.Close()
		return err
	}

	e.log = newFile
	e.keydir = newKeyDir
	return nil
}

// getAt reads value at a specific offset
func (e *DiskEngine) getAt(entry keyDirEntry) ([]byte, error) {
	keyLenBuf := make([]byte, 4)
	_, err := e.log.ReadAt(keyLenBuf, entry.offset)
	if err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	value := make([]byte, entry.valueLen)
	_, err = e.log.ReadAt(value, entry.offset+8+int64(keyLen))
	if err != nil {
		return nil, err
	}

	return value, nil
}

// DiskIterator is an iterator over disk engine
type DiskIterator struct {
	keys   [][]byte
	values [][]byte
	pos    int
	err    error
}

// Next advances the iterator
func (it *DiskIterator) Next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

// Key returns the current key
func (it *DiskIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return it.keys[it.pos]
}

// Value returns the current value
func (it *DiskIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.values) {
		return nil
	}
	return it.values[it.pos]
}

// Err returns any error encountered
func (it *DiskIterator) Err() error {
	return it.err
}

// Close closes the iterator
func (it *DiskIterator) Close() {}
