package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type StreamValue struct {
	id StreamId
	map0 map[string]string
}

type StreamId struct {
	id0 int
    id1 int
}

func (p StreamId) String() string {
	return fmt.Sprintf("%d-%d", p.id0, p.id1)
}

type StreamIdPre struct {
	StreamId
	typ int
}

func xadd(args []Value) Value {
	var err error
	if len(args) < 2 {
		return Value{typ: "error", str: "XADD requires at least 2 arguments"}
	}
	if len(args) % 2 != 0 {
		return Value{typ: "error", str: "XADD requires an even number of arguments"}
	}
	streamKey := args[0].str
	stream, found := globalMap[streamKey]
	if !found {
		stream = &MapValue{typ: "stream", chans: []chan struct{}{}, stream: []StreamValue{}}
		globalMap[streamKey] = stream
	}
	idStr := args[1].str
	var idPre StreamIdPre
	if idStr == "*" {
		idPre = StreamIdPre{StreamId: StreamId{int(time.Now().UnixMilli()), 0}, typ: 1}
	} else {
		idPre, err = parseStreamId(idStr)
		if err != nil {
			return Value{typ: "error", str: "Invalid stream id"}
		}	
	}
	id := idPre.StreamId
	if idPre.typ == 1 {
		if stream.lastId.id0 == idPre.id0 {
		id.id1 = stream.lastId.id1 + 1
		} else {
			id.id1 = 0
		}
	}
	err = validateStreamKey(id, stream.lastId)
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}
	map0 := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		map0[args[i].str] = args[i+1].str
	}
	stream.stream = append(stream.stream, StreamValue{id: id, map0: map0})
	stream.lastId = id
	for _, r := range(stream.chans) {
		r <- struct{}{}
	}
	return Value{typ: "bstring", str: id.String()}
}

func parseStreamId(id string) (StreamIdPre, error) {
	idSplit := strings.Split(id, "-")
	if len(idSplit) != 2 {
		return StreamIdPre{}, fmt.Errorf("invalid id format: %s", id)
	}
	id0, err := strconv.Atoi(idSplit[0])
	if err != nil {
		return StreamIdPre{}, fmt.Errorf("error parsing id0: %w", err)
	}
	if idSplit[1] == "*" {
		return StreamIdPre{StreamId: StreamId{id0: id0}, typ: 1}, nil
	}
	id1, err := strconv.Atoi(idSplit[1])
	if err != nil {
		return StreamIdPre{}, fmt.Errorf("error parsing id1: %w", err)
	}
	return StreamIdPre{StreamId: StreamId{id0: id0, id1: id1}, typ: 0}, nil
}

func validateStreamKey(id StreamId, lastId StreamId) error {
	if id.id0 < 0 || id.id1 < 0 || id.id0 == 0 && id.id1 <= 0 {
		return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if id.id0 < lastId.id0 || id.id0 == lastId.id0 && id.id1 == lastId.id1 {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	if id.id0 == lastId.id0 && id.id1 <= lastId.id1 {
		return fmt.Errorf("ERR The ID specified in XADD must be greater than %d-%d", lastId.id0, lastId.id1)
	}
	return nil
}

func xrange(args []Value) Value {
	var err error
	var res Value = Value{typ: "array", arr: []Value{}}
	if len(args) < 3 {
		return Value{typ: "error", str: "XRANGE requires 3 arguments"}
	}
	streamKey := args[0].str
	start := args[1].str
	end := args[2].str
	stream, found := globalMap[streamKey]
	if !found {
		return Value{typ: "bstring", str: ""}
	}
	if start == "-" {
		start = "0-0"
	}
	startId, err := parseStreamId(start)
	if err != nil {
		return Value{typ: "error", str: "Invalid start id"}
	}
	if end == "+" {
		end = fmt.Sprintf("%d-%d", math.MaxInt, math.MaxInt)
	}
	endId, err := parseStreamId(end)
	if err != nil {
		return Value{typ: "error", str: "Invalid end id"}
	}
	for _, s := range stream.stream {
		// If s >= start and s <= end
		if !lessThan(s.id, startId.StreamId) && !greaterThan(s.id, endId.StreamId) {
			d := Value{typ: "array", arr: []Value{}}
			for k, v := range s.map0 {
				d.arr = append(d.arr, Value{typ: "bstring", str: k})
				d.arr = append(d.arr, Value{typ: "bstring", str: v})
			}
			kk := Value{typ: "array", arr: []Value{{typ: "bstring", str: s.id.String()}, d}}
			res.arr = append(res.arr, kk)
		}
	}
	return res
}

func lessThan(p1, p2 StreamId) bool {
	if p1.id0 < p2.id0 {
		return true
	}
	if p1.id0 > p2.id0 {
		return false
	}
	return p1.id1 < p2.id1
}

func greaterThan(p1, p2 StreamId) bool {
	if p1.id0 > p2.id0 {
		return true
	}
	if p1.id0 < p2.id0 {
		return false
	}
	return p1.id1 > p2.id1
}

func xread(args []Value) Value {
	var err error
	streamsPos := 0
	block := -1
	if len(args) < 3 {
		return Value{typ: "error", str: "XREAD requires at least 3 arguments"}
	}
	if args[0].str == "block" {
		if len(args) < 5 {
			return Value{typ: "error", str: "XREAD with block requires at least 5 arguments"}
		}
		block, err = strconv.Atoi(args[1].str)
		if err != nil {
			return Value{typ: "error", str: "Invalid block argument"}
		}
		streamsPos = 2
	}
	if args[streamsPos].str != "streams" {
		return Value{typ: "error", str: "Expected streams keyword"}
	}
	streamKeys, seens := parseXreadStreamKeys(args, streamsPos)
	c, ress := doXread(streamKeys, seens)
	if block < 0 {
		return ress
	}
	if c > 0 {
		return ress
	}
	ch := make(chan struct{})
	for _, streamKey := range(streamKeys) {
		stream, found := globalMap[streamKey]
		if !found {
			continue
		}
		stream.chans = append(stream.chans, ch)
	}
    after := time.Duration(math.MaxInt64)
	if block > 0 {
		after = time.Duration(block) * time.Millisecond
	}
	select {
	case <-ch:
		_, z := doXread(streamKeys, seens)
		for _, streamKey := range(streamKeys) {
			stream, found := globalMap[streamKey]
			if !found {
				continue
			}
			stream.chans = remove(stream.chans, ch)
		}
		close(ch)
		return z
	case <-time.After(after):
		return Value{typ: "bstring", str: ""}
	}
}

func parseXreadStreamKeys(args []Value, streamsPos int) ([]string, []string) {
	streamCount := (len(args) - 1 - streamsPos) / 2
	streamKeys := make([]string, streamCount)
	seens := make([]string, streamCount)
	streamKeysPos := streamsPos + 1
	seensPos := streamKeysPos + streamCount
	for i := 0; i < streamCount; i++ {
		streamKeys[i] = args[streamKeysPos+i].str
		seens[i] = args[seensPos + i].str
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
		typ: "array",
		arr: []Value{},
	}
	for i, streamKey := range(streamKeys) {
		var res Value = Value{typ: "array", arr: []Value{}}
		stream, found := globalMap[streamKey]
		if !found {
			return c, Value{typ: "bstring", str: ""}
		}
		seenId, err := parseStreamId(seens[i])
		if err != nil {
			return c, Value{typ: "error", str: "Invalid seen id: " + seens[i]}
		}
		for _, s := range stream.stream {

			if greaterThan(s.id, seenId.StreamId) {
				d := Value{typ: "array", arr: []Value{}}
				for k, v := range s.map0 {
					d.arr = append(d.arr, Value{typ: "bstring", str: k})
					d.arr = append(d.arr, Value{typ: "bstring", str: v})
				}
				kk := Value{typ: "array", arr: []Value{{typ: "bstring", str: s.id.String()}, d}}
				res.arr = append(res.arr, kk)
				c++
			}
		}
		ress.arr = append(ress.arr, Value{
			typ: "array",
			arr: []Value{
				{typ: "bstring", str: streamKey},
				res,
			},
		})
	}
	return c, ress
}
