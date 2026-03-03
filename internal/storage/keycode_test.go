package storage

import (
	"bytes"
	"testing"
)

func TestKeyCodec_EncodeDecodeString(t *testing.T) {
	kc := NewKeyCodec()

	testCases := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"simple string", "hello"},
		{"string with spaces", "hello world"},
		{"single character", "a"},
		{"unicode string", "hello world"},
		{"long string", "this is a very long string that tests the encoder"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := kc.EncodeString(tc.input)
			decoded, bytesRead := kc.DecodeString(encoded)

			if decoded != tc.input {
				t.Errorf("DecodeString returned wrong value: got %q, want %q", decoded, tc.input)
			}

			if bytesRead != len(encoded) {
				t.Errorf("DecodeString returned wrong bytes read: got %d, want %d", bytesRead, len(encoded))
			}
		})
	}
}

func TestKeyCodec_EncodeDecodeStringWithNullBytes(t *testing.T) {
	kc := NewKeyCodec()

	// Test string with null bytes (escaping)
	testCases := []struct {
		name  string
		input string
	}{
		{"single null", "\x00"},
		{"null at start", "\x00abc"},
		{"null at end", "abc\x00"},
		{"null in middle", "a\x00b\x00c"},
		{"multiple nulls", "\x00\x00\x00"},
		{"double null", "\x00\x00"},
		{"mixed", "a\x00b\x00\x00c"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := kc.EncodeString(tc.input)
			decoded, bytesRead := kc.DecodeString(encoded)

			if decoded != tc.input {
				t.Errorf("DecodeString returned wrong value: got %q (len=%d), want %q (len=%d)",
					decoded, len(decoded), tc.input, len(tc.input))
			}

			if bytesRead != len(encoded) {
				t.Errorf("DecodeString returned wrong bytes read: got %d, want %d", bytesRead, len(encoded))
			}
		})
	}
}

func TestKeyCodec_EncodeStringTerminator(t *testing.T) {
	kc := NewKeyCodec()

	// Verify that encoded strings end with 0x00 0x00
	testCases := []string{"", "a", "abc", "hello world"}

	for _, tc := range testCases {
		encoded := kc.EncodeString(tc)
		if len(encoded) < 2 {
			t.Errorf("Encoded string too short: %v", encoded)
			continue
		}

		if encoded[len(encoded)-2] != 0 || encoded[len(encoded)-1] != 0 {
			t.Errorf("Encoded string does not end with 0x00 0x00: %v", encoded)
		}
	}
}

func TestKeyCodec_EncodeStringNullEscaping(t *testing.T) {
	kc := NewKeyCodec()

	// Test that null bytes are escaped correctly
	// Original: 97 98 0 99 -> Encoded: 97 98 0 255 99 0 0
	input := "ab\x00c"
	encoded := kc.EncodeString(input)

	// Expected: 'a' 'b' 0 255 'c' 0 0
	expected := []byte{'a', 'b', 0, 0xFF, 'c', 0, 0}
	if !bytes.Equal(encoded, expected) {
		t.Errorf("EncodeString null escaping wrong: got %v, want %v", encoded, expected)
	}

	// Test double null in input
	// Original: 97 98 0 0 99 -> Encoded: 97 98 0 255 0 255 99 0 0
	input2 := "ab\x00\x00c"
	encoded2 := kc.EncodeString(input2)
	expected2 := []byte{'a', 'b', 0, 0xFF, 0, 0xFF, 'c', 0, 0}
	if !bytes.Equal(encoded2, expected2) {
		t.Errorf("EncodeString double null escaping wrong: got %v, want %v", encoded2, expected2)
	}
}

func TestKeyCodec_EncodeDecodeInt64(t *testing.T) {
	kc := NewKeyCodec()

	testCases := []struct {
		name  string
		input int64
	}{
		{"zero", 0},
		{"positive one", 1},
		{"negative one", -1},
		{"positive large", 9223372036854775807},  // max int64
		{"negative large", -9223372036854775808}, // min int64
		{"positive medium", 123456789},
		{"negative medium", -123456789},
		{"positive small", 42},
		{"negative small", -42},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := kc.EncodeInt64(tc.input)
			if len(encoded) != 8 {
				t.Errorf("EncodeInt64 returned wrong length: got %d, want 8", len(encoded))
			}

			decoded := kc.DecodeInt64(encoded)
			if decoded != tc.input {
				t.Errorf("DecodeInt64 returned wrong value: got %d, want %d", decoded, tc.input)
			}
		})
	}
}

func TestKeyCodec_EncodeInt64Ordering(t *testing.T) {
	kc := NewKeyCodec()

	// Test that encoded values preserve ordering
	testCases := []int64{
		-9223372036854775808, // min int64
		-1000,
		-1,
		0,
		1,
		1000,
		9223372036854775807, // max int64
	}

	var encoded [][]byte
	for _, tc := range testCases {
		encoded = append(encoded, kc.EncodeInt64(tc))
	}

	// Verify ordering is preserved
	for i := 0; i < len(encoded)-1; i++ {
		if bytes.Compare(encoded[i], encoded[i+1]) >= 0 {
			t.Errorf("Ordering not preserved: %d should come before %d", testCases[i], testCases[i+1])
		}
	}
}

func TestKeyCodec_EncodeInt64PositiveVsNegative(t *testing.T) {
	kc := NewKeyCodec()

	// Positive numbers should sort after negative numbers
	negOne := kc.EncodeInt64(-1)
	posOne := kc.EncodeInt64(1)

	if bytes.Compare(negOne, posOne) >= 0 {
		t.Error("Negative numbers should sort before positive numbers")
	}

	// Zero should sort between negative and positive
	zero := kc.EncodeInt64(0)
	if bytes.Compare(negOne, zero) >= 0 {
		t.Error("-1 should sort before 0")
	}
	if bytes.Compare(zero, posOne) >= 0 {
		t.Error("0 should sort before 1")
	}
}

func TestKeyCodec_EncodeInt64SpecificValues(t *testing.T) {
	kc := NewKeyCodec()

	// Test specific encoding values
	// The encoding flips the sign bit, so:
	// - Int64 0 -> Uint64 0x8000000000000000 (sign bit flipped)
	// - Int64 1 -> Uint64 0x8000000000000001
	// - Int64 -1 -> Uint64 0x7FFFFFFFFFFFFFFF

	zero := kc.EncodeInt64(0)
	one := kc.EncodeInt64(1)
	negOne := kc.EncodeInt64(-1)

	// Verify ordering: -1 < 0 < 1
	if bytes.Compare(negOne, zero) >= 0 {
		t.Error("-1 should encode to sort before 0")
	}
	if bytes.Compare(zero, one) >= 0 {
		t.Error("0 should encode to sort before 1")
	}

	// Verify decoding
	if kc.DecodeInt64(zero) != 0 {
		t.Error("Decoding zero failed")
	}
	if kc.DecodeInt64(one) != 1 {
		t.Error("Decoding one failed")
	}
	if kc.DecodeInt64(negOne) != -1 {
		t.Error("Decoding -1 failed")
	}
}

func TestKeyCodec_Concat(t *testing.T) {
	kc := NewKeyCodec()

	parts := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}

	result := kc.Concat(parts...)
	expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}

	if !bytes.Equal(result, expected) {
		t.Errorf("Concat returned wrong result: got %v, want %v", result, expected)
	}

	// Test empty parts
	empty := kc.Concat()
	if len(empty) != 0 {
		t.Errorf("Concat with no parts should return empty slice, got %v", empty)
	}

	// Test single part
	single := kc.Concat([]byte{1, 2, 3})
	if !bytes.Equal(single, []byte{1, 2, 3}) {
		t.Errorf("Concat with single part returned wrong result: got %v, want %v", single, []byte{1, 2, 3})
	}
}

func TestKeyCodec_RoundTripCombined(t *testing.T) {
	kc := NewKeyCodec()

	// Test combining string and int64 encoding
	str := "testkey"
	num := int64(42)

	encodedStr := kc.EncodeString(str)
	encodedNum := kc.EncodeInt64(num)

	combined := kc.Concat(encodedStr, encodedNum)

	// Decode string part
	decodedStr, bytesRead := kc.DecodeString(combined)
	if decodedStr != str {
		t.Errorf("Decoded string wrong: got %q, want %q", decodedStr, str)
	}

	// Decode int64 part
	decodedNum := kc.DecodeInt64(combined[bytesRead:])
	if decodedNum != num {
		t.Errorf("Decoded int64 wrong: got %d, want %d", decodedNum, num)
	}
}

func TestKeyCodec_StringOrdering(t *testing.T) {
	kc := NewKeyCodec()

	// Test that encoded strings preserve lexicographic ordering
	testCases := []string{
		"",
		"a",
		"aa",
		"ab",
		"b",
		"bb",
	}

	var encoded [][]byte
	for _, tc := range testCases {
		encoded = append(encoded, kc.EncodeString(tc))
	}

	// Verify ordering is preserved
	for i := 0; i < len(encoded)-1; i++ {
		if bytes.Compare(encoded[i], encoded[i+1]) >= 0 {
			t.Errorf("String ordering not preserved: %q should come before %q", testCases[i], testCases[i+1])
		}
	}
}

func TestKeyCodec_StringOrderingWithNullBytes(t *testing.T) {
	kc := NewKeyCodec()

	// Test that strings with null bytes maintain proper ordering
	testCases := []string{
		"",
		"a",
		"a\x00",
		"a\x00b",
		"ab",
		"b",
	}

	var encoded [][]byte
	for _, tc := range testCases {
		encoded = append(encoded, kc.EncodeString(tc))
	}

	// Verify ordering is preserved
	for i := 0; i < len(encoded)-1; i++ {
		if bytes.Compare(encoded[i], encoded[i+1]) >= 0 {
			t.Errorf("String ordering with nulls not preserved: %q (encoded: %v) should come before %q (encoded: %v)",
				testCases[i], encoded[i], testCases[i+1], encoded[i+1])
		}
	}
}
