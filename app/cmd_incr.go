package main

import (
	"strconv"
)

func incrCommand(req *Value) Value {
	var i int
	var err error
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "INCR requires at least 1 argument"}
	}
	key := args[1].Str
	val, ok := GlobalMap[key]
	if !ok {
		i = 1
	} else {
		if val.Typ != "string" {
			return Value{Typ: "error", Str: "Value is not a string"}
		}
		i, err = strconv.Atoi(val.Str)
		if err != nil {
			return Value{Typ: "error", Str: "ERR value is not an integer or out of range"}
		}
		i++
	}
	GlobalMap[key] = &MapValue{Typ: "string", Str: strconv.Itoa(i)}
	sendCommandToReplicas(req)
	return Value{Typ: "int", Int: i}
}

