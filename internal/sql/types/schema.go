package types

import "encoding/json"

// Column represents a table column definition
type Column struct {
	Name       string   `json:"name"`
	DataType   DataType `json:"data_type"`
	Nullable   bool     `json:"nullable"`
	Default    *Value   `json:"default"`
	PrimaryKey bool     `json:"primary_key"`
}

// Table represents a table schema
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// GetPrimaryKey returns the primary key column index
func (t *Table) GetPrimaryKey() (int, bool) {
	for i, col := range t.Columns {
		if col.PrimaryKey {
			return i, true
		}
	}
	return -1, false
}

// GetColumnIndex returns the column index by name
func (t *Table) GetColumnIndex(name string) (int, bool) {
	for i, col := range t.Columns {
		if col.Name == name {
			return i, true
		}
	}
	return -1, false
}

// Serialize serializes the table to JSON bytes
func (t *Table) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// DeserializeTable deserializes JSON bytes to a Table
func DeserializeTable(data []byte) (*Table, error) {
	var table Table
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, err
	}
	return &table, nil
}

// SerializeRow serializes a row to JSON bytes
func SerializeRow(row Row) ([]byte, error) {
	return json.Marshal(row)
}

// DeserializeRow deserializes JSON bytes to a Row
func DeserializeRow(data []byte) (Row, error) {
	var row Row
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	return row, nil
}
