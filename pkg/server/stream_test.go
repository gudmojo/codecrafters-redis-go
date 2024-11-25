package server

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
				{Typ: "bstring", Str: "streams"},
				{Typ: "bstring", Str: "stream1"},
				{Typ: "bstring", Str: "stream2"},
				{Typ: "bstring", Str: "1-1"},
				{Typ: "bstring", Str: "2-2"},
			},
			streamsPos:    0,
			expectedKeys:  []string{"stream1", "stream2"},
			expectedSeens: []string{"1-1", "2-2"},
		},
		{
			args: []Value{
				{Typ: "bstring", Str: "block"},
				{Typ: "bstring", Str: "1000"},
				{Typ: "bstring", Str: "streams"},
				{Typ: "bstring", Str: "stream3"},
				{Typ: "bstring", Str: "stream4"},
				{Typ: "bstring", Str: "3-3"},
				{Typ: "bstring", Str: "4-4"},
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
