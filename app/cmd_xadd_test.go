package main

import (
	"testing"
)

func TestValidateStreamKey(t *testing.T) {
	tests := []struct {
		id       StreamId
		lastId   StreamId
		expected string
	}{
		{
			id:       StreamId{1, 0},
			lastId:   StreamId{0, 0},
			expected: "",
		},
		{
			id:       StreamId{0, 1},
			lastId:   StreamId{1, 0},
			expected: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		},
		{
			id:       StreamId{1, 1},
			lastId:   StreamId{1, 0},
			expected: "",
		},
		{
			id:       StreamId{1, 0},
			lastId:   StreamId{1, 1},
			expected: "ERR The ID specified in XADD must be greater than 1-1",
		},
		{
			id:       StreamId{1, 1},
			lastId:   StreamId{1, 1},
			expected: "ERR The ID specified in XADD is equal or smaller than the target stream top item",
		},
	}
	for _, test := range tests {
		err := validateStreamKey(test.id, test.lastId)
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
