package main

import (
	"fmt"
	"math"
	"testing"
)

func TestHeader(t *testing.T) {
	input := []byte{0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31} // REDIS0011
	for i := 0; i < len(input); i++ {
		Log(fmt.Sprintf("%d %d %s", i, input[i], string(input[i]))
	}
}

/*func TestMetadata(t *testing.T) {
	input := []byte{
		0xFA,                                                       // Indicates the start of a metadata subsection.
		0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, // The name of the metadata attribute (string encoded): "redis-ver".
		0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36, // The value of the metadata attribute (string encoded): "6.0.16".
	}
	_, x := parseMetadata(input, 0)
	if len(x.attr) != 1 {
		t.Fatalf("Expected 1 attribute, got %d", len(x.attr))
	}
	if x.attr[0].name != "redis-ver" {
		t.Fatalf("Expected name to be 'redis-ver', got %s", x.attr[0].name)
	}
	if x.attr[0].value != "6.0.16" {
		t.Fatalf("Expected value to be '6.0.16', got %s", x.attr[0].value)
	}
}*/

func TestParseSizeEncoded(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int
	}{
		// If the first two bits are 0b00:
		// The size is the remaining 6 bits of the byte.
		{[]byte{0b00000001}, 1},
		{[]byte{0b00100000}, intPow(2, 5)},
		// If the first two bits are 0b01:
		// The size is the next 14 bits
		{[]byte{0b01000000, 0b00000001}, 1},
		{[]byte{0b01100000, 0b00000000}, intPow(2, 13)},
		// If the first two bits are 0b10:
		// Ignore the remaining 6 bits of the first byte.
		// The size is the next 4 bytes
		{[]byte{0b10000000, 0, 0, 0, 1}, 1},
		{[]byte{0b10001000, 0b01000000, 0b00000000, 0b00000000, 0b00000000}, intPow(2, 30)},
		{[]byte{0b10001000, 0b10000000, 0b00000000, 0b00000000, 0b00000000}, intPow(2, 31)},
		{[]byte{0b10001111, 0b11111111, 0b11111111, 0b11111111, 0b11111111}, math.MaxUint32},
	}

	for i, tt := range tests {
		Log(i)
		_, size, _, err := parseSizeEncoded(tt.input, 0)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if size != uint(tt.expected) {
			t.Fatalf("Expected size %d, got %d", tt.expected, size)
		}
	}
}
func TestIntPow(t *testing.T) {
	tests := []struct {
		base     int
		exp      int
		expected int
	}{
		{2, 0, 1},
		{2, 1, 2},
		{2, 2, 4},
		{2, 3, 8},
		{2, 10, 1024},
		{3, 3, 27},
		{5, 3, 125},
		{10, 5, 100000},
		{7, 2, 49},
		{2, 8, 256},
		{2, 16, 65536},
		{2, 30, 1073741824},
		{2, 31, 2147483648},
		{2, 32, 4294967296},
	}

	for _, tt := range tests {
		result := intPow(tt.base, tt.exp)
		if result != tt.expected {
			t.Fatalf("intPow(%d, %d) = %d; expected %d", tt.base, tt.exp, result, tt.expected)
		}
	}
}

func TestRdbRead(t *testing.T) {
	input := []byte{
		// REDIS0011
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
		// Metadata section
		0xfa, 
		// size=10 "redis-bits"
		0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73,
		// c0 encoded "64"
		0xc0, 0x40,
		// Metadata section
		0xfa,
		// size=9 "redis-ver"
		0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72,
		// size=5 "7.2.0" 
		0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 
		// Database subsection
		0xfe,
		// index=0 
		0x00,
		// hash table size marker
		0xfb,
		// size encoded 1
		0x01,
		// size encoded 0
		0x00,
		// type string
		0x00,
		// key size=5 "apple" 
		0x05, 0x61, 0x70, 0x70, 0x6c, 0x65,
		// value size=5 "grape" 
		0x05, 0x67, 0x72, 0x61, 0x70, 0x65, 
		// File end marker
		0xff,
		// Checksum
		0x40, 0xb7, 0xdc, 0x4f, 0x95, 0x6e, 0x07, 0x91, 
		// Junk to ignore
		0x0a,
	}
	parseRDB(input)
}

func TestRdbRead2(t *testing.T) {
	input := []byte{
		// REDIS0011
		0x52 ,0x45 ,0x44 ,0x49 ,0x53 ,0x30,0x30,0x31,0x31,
		// Metadata section		
		0xfa,
		// size=9 "redis-ver"
		0x09,0x72,0x65,0x64,0x69,0x73,0x2d,0x76,0x65,0x72,
		// size=5 "7.2.0"
        0x05,0x37,0x2e,0x32,0x2e,0x30,
		// Metadata section		
		0xfa,
		// size=10 "redis-bits"
		0x0a,0x72,0x65,0x64,0x69,0x73,0x2d,0x62,0x69,0x74,0x73,
		// c0 encoded "64"
		0xc0,0x40,
		// Database subsection
		0xfe,
		// index=0
		0x00,
		// hash table size marker
		0xfb,

0x01,0x00,0x00,0x09,0x70,
0x69,0x6e,0x65,0x61,0x70,0x70,0x6c,0x65,0x05,0x67,0x72,0x61,0x70,0x65,0xff,0x90,
0xd9,0x1d,0x98,0x42,0x45,0xfa,0x57,0x0a,
	}
	parseRDB(input)
}
