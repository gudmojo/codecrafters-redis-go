package main

import (
	"testing"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{Value{typ: "bstring", str: "hello"}, "$5\n\rhello\r\n"},
		{Value{typ: "string", str: "OK"}, "+OK\r\n"},
		{Value{typ: "error", str: "ERR unknown command"}, "-ERR unknown command\r\n"},
	}

	for _, test := range tests {
		output := serialize(test.input)
		if output != test.expected {
			t.Errorf("serialize(%v) = %v; want %v", test.input, output, test.expected)
		}
	}
}
func TestParse(t *testing.T) {
	tests := []struct {
		input    string
		expected []Value
		err      bool
	}{
		{"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", []Value{{typ:"bstring", str:"ECHO"}, {typ:"bstring", str:"hey"}}, false},
		{"*1\r\n$4\r\nPING\r\n", []Value{{typ:"bstring", str:"PING"}}, false},
		{"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", []Value{{typ:"bstring", str:"SET"}, {typ:"bstring", str:"key"}, {typ:"bstring", str:"value"}}, false},
		{"*2\r\n$4\r\nECHO\r\n$-1\r\n", nil, true},
	}

	for _, test := range tests {
		output, err := parse([]byte(test.input))
		if (err != nil) != test.err {
			t.Fatalf("parse(%v) error = %v; want err = %v", test.input, err, test.err)
		}
		if !test.err && !equal(output, test.expected) {
			t.Errorf("parse(%v) = %v; want %v", test.input, output, test.expected)
		}
	}
}
func TestReadNumber(t *testing.T) {
	tests := []struct {
		input    []byte
		start    int
		expected int
		expectEnd int
		err      bool
	}{
		{[]byte("12345"), 0, 12345, 5, false},
		{[]byte("6789"), 0, 6789, 4, false},
		{[]byte("abc"), 0, 0, 0, true},
		{[]byte("123abc"), 0, 123, 3, false},
		{[]byte(""), 0, 0, 0, true},
	}

	for _, test := range tests {
		le, output, err := ReadNumber(test.input, test.start)
		if (err != nil) != test.err {
			t.Errorf("ReadNumber(%v, %d) error = %v; want err = %v", test.input, test.start, err, test.err)
		}
		if output != test.expected {
			t.Errorf("ReadNumber(%v, %d) = %d; want %d", test.input, test.start, output, test.expected)
		}
		if le != test.expectEnd {
			t.Errorf("ReadNumber() = %d; want %d", le, test.expectEnd)
		}
	}
}

func equal(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

