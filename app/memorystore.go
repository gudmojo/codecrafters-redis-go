package main

import (
	"time"
	"fmt"
)

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)

// Item in the in-memory datastore
type MapValue struct {
	Typ    string
	Exp    time.Time
	Str    string
	Stream []StreamValue
	LastId StreamId
	Chans  []chan struct{}
}

// Item in a stream
type StreamValue struct {
	id StreamId
	map0 map[string]string
}

// Items in a stream are identified by a 2 part id
type StreamId struct {
	id0 int
    id1 int
}

// StreamId that can have a wildcard for id1
type StreamIdPre struct {
	StreamId
	Id1Wildcard bool
}

func (p StreamId) String() string {
	return fmt.Sprintf("%d-%d", p.id0, p.id1)
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
