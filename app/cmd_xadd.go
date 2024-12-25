package main

import (
	"time"
	"strings"
	"strconv"
	"fmt"
)

func xadd(req *Value) Value {
	args := req.Arr
	var err error
	if len(args) < 3 {
		return Value{Typ: "error", Str: "XADD requires at least 2 arguments"}
	}
	if len(args) % 2 != 1 {
		return Value{Typ: "error", Str: "XADD requires an even number of arguments"}
	}
	streamKey := args[1].Str
	stream, found := GlobalMap[streamKey]
	if !found {
		stream = &MapValue{Typ: "stream", Chans: []chan struct{}{}, Stream: []StreamValue{}}
		GlobalMap[streamKey] = stream
	}
	idStr := args[2].Str
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
	// If the id is id0-*, increment the id1 or start from 0
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
	for i := 3; i < len(args); i += 2 {
		map0[args[i].Str] = args[i+1].Str
	}
	stream.Stream = append(stream.Stream, StreamValue{id: id, map0: map0})
	stream.LastId = id
	for _, r := range(stream.Chans) {
		r <- struct{}{}
	}
	return Value{Typ: "bstring", Str: id.String()}
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

// Parse a stream id in the format id0-id1 or id0-*
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
