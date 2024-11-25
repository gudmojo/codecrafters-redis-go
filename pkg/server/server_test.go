package server

import (
	"testing"
	"log"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{Value{Typ: "bstring", Str: "hello"}, "$5\r\nhello\r\n"},
		{Value{Typ: "string", Str: "OK"}, "+OK\r\n"},
		{Value{Typ: "error", Str: "ERR unknown command"}, "-ERR unknown command\r\n"},
	}

	for _, test := range tests {
		output := Serialize(test.input)
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
		{"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", []Value{{Typ:"bstring", Str:"ECHO"}, {Typ:"bstring", Str:"hey"}}, false},
		{"*1\r\n$4\r\nPING\r\n", []Value{{Typ:"bstring", Str:"PING"}}, false},
		{"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", []Value{{Typ:"bstring", Str:"SET"}, {Typ:"bstring", Str:"key"}, {Typ:"bstring", Str:"value"}}, false},
		{"*2\r\n$4\r\nECHO\r\n$-1\r\n", nil, true},
	}

	for _, test := range tests {
		output, err := Parse([]byte(test.input))
		if (err != nil) != test.err {
			t.Fatalf("parse(%v) error = %v; want err = %v", test.input, err, test.err)
		}
		if !test.err && !equalArrays(output, test.expected) {
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
func TestValidateStreamKey(t *testing.T) {
	tests := []struct {
		id      StreamId
		lastId    StreamId
		expected string
	}{
		{
			id:      StreamId{1, 0},
			lastId: StreamId{0, 0},
			expected: "",
		},
		{
			id:      StreamId{0, 1},
			lastId: StreamId{1, 0},
			expected: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		},
		{
			id:      StreamId{1, 1},
			lastId: StreamId{1, 0},
			expected: "",
		},
		{
			id:      StreamId{1, 0},
			lastId: StreamId{1, 1},
			expected: "ERR The ID specified in XADD must be greater than 1-1",
		},
		{
			id:      StreamId{1, 1},
			lastId: StreamId{1, 1},
			expected: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		},
	}

	for i, test := range tests {
		log.Printf("i=%d", i)
		err := validateStreamKey(test.id, test.lastId)
		log.Printf("err=%v", err)
		expect := test.expected
		if expect == "" {
			if err != nil {
				t.Fatalf("validateStreamKey(%v) = %v; want %v", test.id, err, test.expected)
			}
		} else {
			if err == nil {
				t.Fatalf("Expected error, got nil")
			}
			if err.Error() != test.expected {
				t.Fatalf("validateStreamKey(%v) = %v; want %v", test.id, err, test.expected)
			}
		}
	}
}

func equalArrays(a, b []Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equal(a, b Value) bool {
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
