package main

import (
	"testing"
)

func TestParseXreadStreamKeys(t *testing.T) {
	tests := []struct {
		args          []Value
		streamsPos    int
		expectedKeys  []string
		expectedSeens []string
	}{
		{
			args: []Value{
				{typ: "bstring", str: "streams"},
				{typ: "bstring", str: "stream1"},
				{typ: "bstring", str: "stream2"},
				{typ: "bstring", str: "1-1"},
				{typ: "bstring", str: "2-2"},
			},
			streamsPos:    0,
			expectedKeys:  []string{"stream1", "stream2"},
			expectedSeens: []string{"1-1", "2-2"},
		},
		{
			args: []Value{
				{typ: "bstring", str: "block"},
				{typ: "bstring", str: "1000"},
				{typ: "bstring", str: "streams"},
				{typ: "bstring", str: "stream3"},
				{typ: "bstring", str: "stream4"},
				{typ: "bstring", str: "3-3"},
				{typ: "bstring", str: "4-4"},
			},
			streamsPos:    2,
			expectedKeys:  []string{"stream3", "stream4"},
			expectedSeens: []string{"3-3", "4-4"},
		},
	}

	for _, test := range tests {
		streamKeys, seens := parseXreadStreamKeys(test.args, test.streamsPos)
		if !equalStringSlices(streamKeys, test.expectedKeys) {
			t.Errorf("parseXreadStreamKeys(%v, %d) streamKeys = %v; want %v", test.args, test.streamsPos, streamKeys, test.expectedKeys)
		}
		if !equalStringSlices(seens, test.expectedSeens) {
			t.Errorf("parseXreadStreamKeys(%v, %d) seens = %v; want %v", test.args, test.streamsPos, seens, test.expectedSeens)
		}
	}
}

func equalStringSlices(a, b []string) bool {
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
