package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input    string
		expected *Value
		err      bool
	}{
		{"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", &Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "ECHO"}, {Typ: "bstring", Str: "hey"}}}, false},
		{"*1\r\n$4\r\nPING\r\n", &Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}}, false},
		{"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", &Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "SET"}, {Typ: "bstring", Str: "key"}, {Typ: "bstring", Str: "value"}}}, false},
		{"*2\r\n$4\r\nECHO\r\n$-1\r\n", &Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "ECHO"}, {Typ: "bstring", Str: ""}}}, false},
	}

	for j, test := range tests {
		reader := NewParser(bufio.NewReader(bytes.NewReader([]byte(test.input))))
		i, output, err := reader.ParseArrayOfBstringValues()
		if (err != nil) != test.err {
			t.Errorf("parse(%v) error = %v; want err = %v", j, err, test.err)
		}
		if !test.err && !equal(output, test.expected) {
			t.Errorf("parse(%v) = %v; want %v", j, output, test.expected)
		}
		if i != len(test.input) {
			t.Errorf("parse(%v) i = %v; want i = %v", j, i, len(test.input))
		}
	}
}

func TestReadNumber(t *testing.T) {
	tests := []struct {
		input     []byte
		expected  int
		expectEnd int
		err       bool
	}{
		{[]byte("12345"), 12345, 5, false},
		{[]byte("6789"), 6789, 4, false},
		{[]byte("abc"), 0, 0, true},
		{[]byte("123abc"), 123, 3, false},
		{[]byte(""), 0, 0, true},
	}

	for j, test := range tests {
		output, err := ReadNumber(test.input)
		if (err != nil) != test.err {
			t.Errorf("ReadNumber(%d) error = %v; want err = %v", j, err, test.err)
		}
		if output != test.expected {
			t.Errorf("ReadNumber(%d) = %d; want %d", j, output, test.expected)
		}
	}
}

func equalArrays(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equal(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

func equal(a, b *Value) bool {
	if a == nil {
		return b == nil
	} else if b == nil {
		return false
	}

	switch a.Typ {
	case "bstring":
		return b.Typ == "bstring" && a.Str == b.Str
	case "string":
		return b.Typ == "string" && a.Str == b.Str
	case "error":
		return b.Typ == "error" && a.Str == b.Str
	case "array":
		return b.Typ == "array" && equalArrays(a.Arr, b.Arr)
	}
	return false
}
