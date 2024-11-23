package main

import (
	"testing"
	"log"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		input    Value
		expected string
	}{
		{Value{typ: "bstring", str: "hello"}, "$5\r\nhello\r\n"},
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
func TestValidateStreamKey(t *testing.T) {
	tests := []struct {
		id      string
		expected string
		setup    func()
	}{
		{
			id:      "1-0",
			expected: "",
			setup: func() {
				globalMap["stream"] = &MapValue{lastStreamId0: 0, lastStreamId1: 0}
			},
		},
		{
			id:      "0-1",
			expected: "id0 was less than lastId0",
			setup: func() {
				globalMap["stream"] = &MapValue{lastStreamId0: 1, lastStreamId1: 0}
			},
		},
		{
			id:      "1-1",
			expected: "",
			setup: func() {
				globalMap["stream"] = &MapValue{lastStreamId0: 1, lastStreamId1: 0}
			},
		},
		{
			id:      "1-0",
			expected: "id1 must be greater than lastId1 if id0 == lastId0",
			setup: func() {
				globalMap["stream"] = &MapValue{lastStreamId0: 1, lastStreamId1: 1}
			},
		},
		{
			id:      "1-1",
			expected: "id1 must be greater than lastId1 if id0 == lastId0",
			setup: func() {
				globalMap["stream"] = &MapValue{lastStreamId0: 1, lastStreamId1: 1}
			},
		},
		{
			id:      "invalid-key",
			expected: "error parsing id0: strconv.ParseInt: parsing \"invalid\": invalid syntax",
			setup:    func() {},
		},
	}

	for i, test := range tests {
		log.Printf("i=%d", i)
		test.setup()
		err := validateStreamKey("stream", test.id)
		log.Printf("err=%v", err)
		expect := test.expected
		if expect == "" {
			if err != nil {
				t.Errorf("validateStreamKey(%v) a = %v; want %v", test.id, err, test.expected)
			}
		} else {
			if err == nil {
				t.Fatalf("Expected error, got nil")
			}
			if err.Error() != test.expected {
				t.Errorf("validateStreamKey(%v) b = %v; want %v", test.id, err, test.expected)
			}
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

