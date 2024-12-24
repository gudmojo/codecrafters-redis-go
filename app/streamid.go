package main

import (
	"fmt"
	"strconv"
	"strings"
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
