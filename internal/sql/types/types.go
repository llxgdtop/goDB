package types

import (
	"encoding/json"
	"fmt"
)

// DataType represents SQL data types
type DataType int

const (
	TypeBoolean DataType = iota
	TypeInteger
	TypeFloat
	TypeString
)

func (dt DataType) String() string {
	switch dt {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeInteger:
		return "INTEGER"
	case TypeFloat:
		return "FLOAT"
	case TypeString:
		return "STRING"
	default:
		return "UNKNOWN"
	}
}

// Value represents a SQL value
type Value struct {
	Type  DataType
	Null  bool
	Bool  bool
	Int   int64
	Float float64
	Str   string
}

// NewNullValue creates a null value
func NewNullValue() Value {
	return Value{Null: true}
}

// NewBoolValue creates a boolean value
func NewBoolValue(v bool) Value {
	return Value{Type: TypeBoolean, Bool: v}
}

// NewIntValue creates an integer value
func NewIntValue(v int64) Value {
	return Value{Type: TypeInteger, Int: v}
}

// NewFloatValue creates a float value
func NewFloatValue(v float64) Value {
	return Value{Type: TypeFloat, Float: v}
}

// NewStringValue creates a string value
func NewStringValue(v string) Value {
	return Value{Type: TypeString, Str: v}
}

func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	switch v.Type {
	case TypeBoolean:
		return fmt.Sprintf("%v", v.Bool)
	case TypeInteger:
		return fmt.Sprintf("%d", v.Int)
	case TypeFloat:
		return fmt.Sprintf("%v", v.Float)
	case TypeString:
		return v.Str
	default:
		return "UNKNOWN"
	}
}

// Compare compares two values
func (v Value) Compare(other Value) int {
	if v.Null && other.Null {
		return 0
	}
	if v.Null {
		return -1
	}
	if other.Null {
		return 1
	}

	if v.Type != other.Type {
		// Type coercion for comparison
		if v.Type == TypeInteger && other.Type == TypeFloat {
			if float64(v.Int) < other.Float {
				return -1
			} else if float64(v.Int) > other.Float {
				return 1
			}
			return 0
		}
		if v.Type == TypeFloat && other.Type == TypeInteger {
			if v.Float < float64(other.Int) {
				return -1
			} else if v.Float > float64(other.Int) {
				return 1
			}
			return 0
		}
	}

	switch v.Type {
	case TypeBoolean:
		if v.Bool == other.Bool {
			return 0
		}
		if !v.Bool && other.Bool {
			return -1
		}
		return 1
	case TypeInteger:
		if v.Int < other.Int {
			return -1
		} else if v.Int > other.Int {
			return 1
		}
		return 0
	case TypeFloat:
		if v.Float < other.Float {
			return -1
		} else if v.Float > other.Float {
			return 1
		}
		return 0
	case TypeString:
		if v.Str < other.Str {
			return -1
		} else if v.Str > other.Str {
			return 1
		}
		return 0
	}
	return 0
}

// Row represents a row of data
type Row []Value

// MarshalJSON implements json.Marshaler for Value
func (v Value) MarshalJSON() ([]byte, error) {
	if v.Null {
		return json.Marshal(nil)
	}
	switch v.Type {
	case TypeBoolean:
		return json.Marshal(v.Bool)
	case TypeInteger:
		return json.Marshal(v.Int)
	case TypeFloat:
		return json.Marshal(v.Float)
	case TypeString:
		return json.Marshal(v.Str)
	default:
		return json.Marshal(nil)
	}
}

// UnmarshalJSON implements json.Unmarshaler for Value
func (v *Value) UnmarshalJSON(data []byte) error {
	var i interface{}
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}
	switch t := i.(type) {
	case nil:
		v.Null = true
	case bool:
		v.Type = TypeBoolean
		v.Bool = t
	case float64:
		// JSON numbers are always float64
		if float64(int64(t)) == t {
			v.Type = TypeInteger
			v.Int = int64(t)
		} else {
			v.Type = TypeFloat
			v.Float = t
		}
	case string:
		v.Type = TypeString
		v.Str = t
	}
	return nil
}
