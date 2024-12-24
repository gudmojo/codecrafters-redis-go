package main

import (
	"math"
	"strconv"
	"time"
)

func xread(req *Value) Value {
	var err error
	args := req.Arr
	streamsPos := 1
	block := -1
	if len(args) < 4 {
		return Value{Typ: "error", Str: "XREAD requires at least 3 arguments"}
	}
	if args[1].Str == "block" {
		if len(args) < 6 {
			return Value{Typ: "error", Str: "XREAD with block requires at least 5 arguments"}
		}
		block, err = strconv.Atoi(args[2].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Invalid block argument"}
		}
		streamsPos = 3
	}
	if args[streamsPos].Str != "streams" {
		return Value{Typ: "error", Str: "Expected streams keyword"}
	}
	streamKeys, seens := parseXreadStreamKeys(args, streamsPos)
	for i, streamKey := range(streamKeys) {
		stream, found := GlobalMap[streamKey]
		if !found {
			continue
		}
		if seens[i] == "$" {
			seens[i] = stream.LastId.String()
		}
	}
	c, ress := doXread(streamKeys, seens)
	if block < 0 {
		return ress
	}
	if c > 0 {
		return ress
	}
	ch := make(chan struct{})
	for _, streamKey := range(streamKeys) {
		stream, found := GlobalMap[streamKey]
		if !found {
			continue
		}
		stream.Chans = append(stream.Chans, ch)
	}
    after := time.Duration(math.MaxInt64)
	if block > 0 {
		after = time.Duration(block) * time.Millisecond
	}
	select {
	case <-ch:
		_, z := doXread(streamKeys, seens)
		for _, streamKey := range(streamKeys) {
			stream, found := GlobalMap[streamKey]
			if !found {
				continue
			}
			stream.Chans = remove(stream.Chans, ch)
		}
		close(ch)
		return z
	case <-time.After(after):
		return Value{Typ: "bstring", Str: ""}
	}
}

func parseXreadStreamKeys(args []Value, streamsPos int) ([]string, []string) {
	streamCount := (len(args) - 1 - streamsPos) / 2
	streamKeys := make([]string, streamCount)
	seens := make([]string, streamCount)
	streamKeysPos := streamsPos + 1
	seensPos := streamKeysPos + streamCount
	for i := 0; i < streamCount; i++ {
		streamKeys[i] = args[streamKeysPos+i].Str
		seens[i] = args[seensPos + i].Str
	}
	return streamKeys, seens
}

func remove(s []chan struct{}, c chan struct{}) []chan struct{} {
	for i, v := range s {
		if v == c {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func doXread(streamKeys []string, seens []string) (int, Value) {
	c := 0
	ress := Value{
		Typ: "array",
		Arr: []Value{},
	}
	for i, streamKey := range(streamKeys) {
		var res Value = Value{Typ: "array", Arr: []Value{}}
		stream, found := GlobalMap[streamKey]
		if !found {
			return c, Value{Typ: "bstring", Str: ""}
		}
		seenId, err := parseStreamId(seens[i])
		if err != nil {
			return c, Value{Typ: "error", Str: "Invalid seen id: " + seens[i]}
		}
		for _, s := range stream.Stream {

			if greaterThan(s.id, seenId.StreamId) {
				d := Value{Typ: "array", Arr: []Value{}}
				for k, v := range s.map0 {
					d.Arr = append(d.Arr, Value{Typ: "bstring", Str: k})
					d.Arr = append(d.Arr, Value{Typ: "bstring", Str: v})
				}
				kk := Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: s.id.String()}, d}}
				res.Arr = append(res.Arr, kk)
				c++
			}
		}
		ress.Arr = append(ress.Arr, Value{
			Typ: "array",
			Arr: []Value{
				{Typ: "bstring", Str: streamKey},
				res,
			},
		})
	}
	return c, ress
}
