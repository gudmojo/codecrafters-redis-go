package server

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
		return Value{Typ: "error", Str: "XADD requires at least 2 arguments"}
	}
	if len(args) % 2 != 0 {
		return Value{Typ: "error", Str: "XADD requires an even number of arguments"}
	}
	streamKey := args[0].Str
	stream, found := GlobalMap[streamKey]
	if !found {
		stream = &MapValue{Typ: "stream", Chans: []chan struct{}{}, Stream: []StreamValue{}}
		GlobalMap[streamKey] = stream
	}
	idStr := args[1].Str
	var idPre StreamIdPre
	if idStr == "*" {
		idPre = StreamIdPre{StreamId: StreamId{int(time.Now().UnixMilli()), 0}, typ: 1}
	} else {
		idPre, err = parseStreamId(idStr)
		if err != nil {
			return Value{Typ: "error", Str: "Invalid stream id"}
		}	
	}
	id := idPre.StreamId
	if idPre.typ == 1 {
		if stream.LastId.id0 == idPre.id0 {
		id.id1 = stream.LastId.id1 + 1
		} else {
			id.id1 = 0
		}
	}
	err = validateStreamKey(id, stream.LastId)
	if err != nil {
		return Value{Typ: "error", Str: err.Error()}
	}
	map0 := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		map0[args[i].Str] = args[i+1].Str
	}
	stream.Stream = append(stream.Stream, StreamValue{id: id, map0: map0})
	stream.LastId = id
	for _, r := range(stream.Chans) {
		r <- struct{}{}
	}
	return Value{Typ: "bstring", Str: id.String()}
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
	var res Value = Value{Typ: "array", Arr: []Value{}}
	if len(args) < 3 {
		return Value{Typ: "error", Str: "XRANGE requires 3 arguments"}
	}
	streamKey := args[0].Str
	start := args[1].Str
	end := args[2].Str
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
		return Value{Typ: "error", Str: "XREAD requires at least 3 arguments"}
	}
	if args[0].Str == "block" {
		if len(args) < 5 {
			return Value{Typ: "error", Str: "XREAD with block requires at least 5 arguments"}
		}
		block, err = strconv.Atoi(args[1].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Invalid block argument"}
		}
		streamsPos = 2
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
