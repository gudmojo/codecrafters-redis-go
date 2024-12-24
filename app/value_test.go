package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{Value{Typ: "bstring", Str: "hello"}, "$5\r\nhello\r\n"},
		{Value{Typ: "string", Str: "OK"}, "+OK\r\n"},
		{Value{Typ: "error", Str: "ERR unknown command"}, "-ERR unknown command\r\n"},
		{Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "asdf"}, {Typ: "bstring", Str: "qwerty"}}}, "*2\r\n$4\r\nasdf\r\n$6\r\nqwerty\r\n"},
	}

	for _, test := range tests {
		output := Serialize(test.input)
		if string(output) != test.expected {
			t.Errorf("serialize(%v) = %v; want %v", test.input, output, test.expected)
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		input    string
		expected *Value
		err      bool
	}{
		{"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", &Value{Typ: "array", Arr: []Value{{Typ:"bstring", Str:"ECHO"}, {Typ:"bstring", Str:"hey"}}}, false},
		{"*1\r\n$4\r\nPING\r\n", &Value{Typ: "array", Arr:[]Value{{Typ:"bstring", Str:"PING"}}}, false},
		{"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", &Value{Typ: "array", Arr:[]Value{{Typ:"bstring", Str:"SET"}, {Typ:"bstring", Str:"key"}, {Typ:"bstring", Str:"value"}}}, false},
		{"*2\r\n$4\r\nECHO\r\n$-1\r\n", &Value{Typ: "array", Arr:[]Value{{Typ:"bstring", Str:"ECHO"}, {Typ:"bstring", Str:""}}}, false},
	}

	for j, test := range tests {
		reader := NewReader(bufio.NewReader(bytes.NewReader([]byte(test.input))))
		i, output, err := reader.ParseArrayOfBstringValues()
		if (err != nil) != test.err {
			t.Errorf("parse(%v) error = %v; want err = %v", j, err, test.err)
		}
		if !test.err && !equal(output, test.expected) {
			t.Errorf("parse(%v) = %v; want %v", j, output, test.expected)
		}
		if (i != len(test.input)) {
			t.Errorf("parse(%v) i = %v; want i = %v", j, i, len(test.input))
		}
	}
}
