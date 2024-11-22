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
		input    []byte
		expected []Value
		err      bool
	}{
		{[]byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"), []Value{{typ:"bstring", str:"ECHO"}, {typ:"bstring", str:"hey"}}, false},
		{[]byte("*1\r\n$4\r\nPING\r\n"), []Value{{typ:"bstring", str:"PING"}}, false},
//		{[]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"), []Value{{typ:"bstring", str:"SET"}, {typ:"bstring", str:"key"}, {typ:"bstring", str:"value"}}, false},
//		{[]byte("*2\r\n$4\r\nECHO\r\n$-1\r\n"), nil, true},
	}

	for _, test := range tests {
		output, err := parse(test.input)
		if (err != nil) != test.err {
			t.Errorf("parse(%v) error = %v; want err = %v", test.input, err, test.err)
		}
		if !test.err && !equal(output, test.expected) {
			t.Errorf("parse(%v) = %v; want %v", test.input, output, test.expected)
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

