package main

type Value struct {
	Typ         string
	Int         int
	Str         string
	Arr         []Value
	PsyncHeader *Value
	PsyncData   *Value
	Bytes       []byte
}
