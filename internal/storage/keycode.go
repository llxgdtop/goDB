package storage

import (
	"bytes"
	"encoding/binary"
	"math"
)

// KeyCodec provides ordered encoding/decoding for keys
type KeyCodec struct{}

// NewKeyCodec creates a new KeyCodec
func NewKeyCodec() *KeyCodec {
	return &KeyCodec{}
}

// EncodeString encodes a string to ordered bytes
// Uses 0x00 0x00 as terminator, 0x00 is escaped to 0x00 0xFF
func (kc *KeyCodec) EncodeString(s string) []byte {
	var buf bytes.Buffer
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			buf.WriteByte(0)
			buf.WriteByte(0xFF)
		} else {
			buf.WriteByte(s[i])
		}
	}
	buf.WriteByte(0)
	buf.WriteByte(0)
	return buf.Bytes()
}

// DecodeString decodes ordered bytes to a string
func (kc *KeyCodec) DecodeString(data []byte) (string, int) {
	var buf bytes.Buffer
	i := 0
	for i < len(data) {
		if data[i] == 0 {
			if i+1 < len(data) && data[i+1] == 0 {
				// End marker
				return buf.String(), i + 2
			} else if i+1 < len(data) && data[i+1] == 0xFF {
				// Escaped null
				buf.WriteByte(0)
				i += 2
			} else {
				i++
			}
		} else {
			buf.WriteByte(data[i])
			i++
		}
	}
	return buf.String(), i
}

// EncodeInt64 encodes an int64 to ordered bytes (big-endian)
func (kc *KeyCodec) EncodeInt64(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n)^0x8000000000000000) // Flip sign bit for ordering
	return buf
}

// DecodeInt64 decodes ordered bytes to an int64
func (kc *KeyCodec) DecodeInt64(data []byte) int64 {
	n := binary.BigEndian.Uint64(data)
	return int64(n ^ 0x8000000000000000) // Flip sign bit back
}

// EncodeFloat64 encodes a float64 to ordered bytes
// Uses IEEE 754 big-endian with sign bit flipped for proper ordering
func (kc *KeyCodec) EncodeFloat64(f float64) []byte {
	buf := make([]byte, 8)
	// First convert float64 to its IEEE 754 representation
	bits := math.Float64bits(f)
	// Flip sign bit for proper ordering (negative numbers sort before positive)
	binary.BigEndian.PutUint64(buf, bits^0x8000000000000000)
	return buf
}

// DecodeFloat64 decodes ordered bytes to a float64
func (kc *KeyCodec) DecodeFloat64(data []byte) float64 {
	bits := binary.BigEndian.Uint64(data)
	// Flip sign bit back
	bits ^= 0x8000000000000000
	return math.Float64frombits(bits)
}

// Concat concatenates multiple byte slices
func (kc *KeyCodec) Concat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}
