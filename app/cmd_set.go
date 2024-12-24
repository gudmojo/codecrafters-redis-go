package main

import (
	"fmt"
	"strconv"
	"time"
)

func setCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "SET requires at least 2 arguments"}
	}
	if len(args) == 3 {
		key := args[1].Str
		value := args[2].Str
		GlobalMap[key] = &MapValue{Typ: "string", Str: value}
		sendCommandToReplicas(req)
		return Value{Typ: "string", Str: "OK"}
	}
	if len(args) == 5 {
		key := args[1].Str
		value := args[2].Str
		exp := args[4].Str
		currentTime := time.Now()
		ms, ok := strconv.Atoi(exp)
		if ok != nil {
			return Value{Typ: "error", Str: "Error parsing milliseconds"}
		}
		futureTime := currentTime.Add(time.Duration(ms) * time.Millisecond)
		GlobalMap[key] = &MapValue{Typ: "string", Str: value, Exp: futureTime}
		Log(fmt.Sprintf("SET key: %s, value: %s, exp: %s", key, value, futureTime))
		sendCommandToReplicas(req)
		return Value{Typ: "string", Str: "OK"}
	}
	return Value{Typ: "error", Str: "SET requires 2 or 4 arguments"}
}

