package main

import (
	"time"
	"fmt"
)

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)

type MapValue struct {
	Typ    string
	Exp    time.Time
	Str    string
	Stream []StreamValue
	LastId StreamId
	Chans  []chan struct{}
}

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
