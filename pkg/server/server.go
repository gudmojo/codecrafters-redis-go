package server

import (
	"log"
	"strconv"
	"strings"
	"time"
)

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)
var ConfigDir = ""
var ConfigDbFilename = ""

type MapValue struct {
	Typ string
	Exp time.Time
	Str string
	Stream []StreamValue
	LastId StreamId
	Chans []chan struct{}
}

func HandleCommand(cmd []Value) Value {
	switch strings.ToUpper(cmd[0].Str) {
	case "PING":
		return ping()
	case "ECHO":
		return echo(cmd[1].Str)
	case "SET":
		return set(cmd[1:])
	case "GET":
		return get(cmd[1].Str)
	case "TYPE":
		return type0(cmd[1].Str)
	case "XADD":
		return xadd(cmd[1:])
	case "XRANGE":
		return xrange(cmd[1:])
	case "XREAD":
		return xread(cmd[1:])
	case "CONFIG":
		return config(cmd[1:])
	case "KEYS":
		return keys(cmd[1:])
	}
	return Value{Typ: "error", Str: "Unknown command"}
}

func ping() Value {
	return Value{Typ: "string", Str: "PONG"}
}

func echo(arg string) Value {
	return Value{Typ: "string", Str: arg}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "SET requires at least 2 arguments"}
	}
	if len(args) == 2 {
		key := args[0].Str
		value := args[1].Str
		GlobalMap[key] = &MapValue{Typ: "string", Str: value}
		log.Printf("SET key: %s, value: %s", key, value)
		return Value{Typ: "string", Str: "OK"}
	}
	if len(args) == 4 {
		key := args[0].Str
		value := args[1].Str
		exp := args[3].Str
		currentTime := time.Now() 
		ms, ok := strconv.Atoi(exp)
		if ok != nil {
			return Value{Typ: "error", Str: "Error parsing milliseconds"}
		}
		futureTime := currentTime.Add(time.Duration(ms) * time.Millisecond)
		GlobalMap[key] = &MapValue{Typ: "string", Str: value, Exp: futureTime}
		log.Printf("SET key: %s, value: %s, exp: %s", key, value, futureTime)
		return Value{Typ: "string", Str: "OK"}
	}
	return Value{Typ: "error", Str: "SET requires 2 or 4 arguments"}
}

func get(key string) Value {
	value, ok := GlobalMap[key]
	if !ok {
		return Value{Typ: "bstring", Str: ""}
	}
	if !value.Exp.IsZero() && value.Exp.Before(time.Now()) {
		delete(GlobalMap, key)
		return Value{Typ: "bstring", Str: ""}
	}
	return Value{Typ: "bstring", Str: value.Str}
}

func type0(key string) Value {
	value, ok := GlobalMap[key]
	if !ok {
		return Value{Typ: "string", Str: "none"}
	}
	if !value.Exp.IsZero() && value.Exp.Before(time.Now()) {
		delete(GlobalMap, key)
		return Value{Typ: "string", Str: "none"}
	}
	return Value{Typ: "string", Str: value.Typ}
}

func config(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "CONFIG requires at least 2 arguments"}
	}
	if args[0].Str == "GET" {
		if args[1].Str == "dir" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dir"}, {Typ: "bstring", Str: ConfigDir}}} 
		}
		if args[1].Str == "dbfilename" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dbfilename"}, {Typ: "bstring", Str: ConfigDbFilename}}} 
		}
	}
	return Value{Typ: "error", Str: "Invalid CONFIG command"}
}

func keys(args []Value) Value {
	if len(args) < 1 {
		return Value{Typ: "error", Str: "KEYS requires at least 1 argument"}
	}
	if args[0].Str != "*" {
		return Value{Typ: "error", Str: "Invalid argument"}
	}
	keys := []Value{}
	for k := range GlobalMap {
		keys = append(keys, Value{Typ: "bstring", Str: k})
	}
	return Value{Typ: "array", Arr: keys}
}
