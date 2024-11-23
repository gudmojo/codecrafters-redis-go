package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamValue struct {
	id string
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
		stream = &MapValue{typ: "stream", stream: []StreamValue{}}
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
	stream.stream = append(stream.stream, StreamValue{id: idStr, map0: map0})
	stream.lastId = id
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