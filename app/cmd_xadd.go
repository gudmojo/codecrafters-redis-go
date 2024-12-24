package main

import (
	"time"
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

