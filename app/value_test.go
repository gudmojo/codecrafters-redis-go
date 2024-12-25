package main

import (
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
		output := test.input.Serialize()
		if string(output) != test.expected {
			t.Errorf("serialize(%v) = %v; want %v", test.input, output, test.expected)
		}
	}
}
