package main

import (
	"fmt"
	"math"
)

func xrange(req *Value) Value {
	var err error
	args := req.Arr
	var res Value = Value{Typ: "array", Arr: []Value{}}
	if len(args) < 4 {
		return Value{Typ: "error", Str: "XRANGE requires 3 arguments"}
	}
	streamKey := args[1].Str
	start := args[2].Str
	end := args[3].Str
	stream, found := GlobalMap[streamKey]
	if !found {
		return Value{Typ: "bstring", Str: ""}
	}
	if start == "-" {
		start = "0-0"
	}
	startId, err := parseStreamId(start)
	if err != nil {
		return Value{Typ: "error", Str: "Invalid start id"}
	}
	if end == "+" {
		end = fmt.Sprintf("%d-%d", math.MaxInt, math.MaxInt)
	}
	endId, err := parseStreamId(end)
	if err != nil {
		return Value{Typ: "error", Str: "Invalid end id"}
	}
	for _, s := range stream.Stream {
		// If s >= start and s <= end
		if !lessThan(s.id, startId.StreamId) && !greaterThan(s.id, endId.StreamId) {
			d := Value{Typ: "array", Arr: []Value{}}
			for k, v := range s.map0 {
				d.Arr = append(d.Arr, Value{Typ: "bstring", Str: k})
				d.Arr = append(d.Arr, Value{Typ: "bstring", Str: v})
			}
			kk := Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: s.id.String()}, d}}
			res.Arr = append(res.Arr, kk)
		}
	}
	return res
}

