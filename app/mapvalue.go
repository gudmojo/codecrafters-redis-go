package main

import (
	"time"
)
type MapValue struct {
	Typ    string
	Exp    time.Time
	Str    string
	Stream []StreamValue
	LastId StreamId
	Chans  []chan struct{}
}
