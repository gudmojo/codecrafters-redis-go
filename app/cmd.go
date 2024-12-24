package main

import (
	"fmt"
	"time"
)

func pingCommand() Value {
	return Value{Typ: "string", Str: "PONG"}
}

func echoCommand(req *Value) Value {
	return Value{Typ: "string", Str: req.Arr[1].Str}
}

func multiCommand(req *Value, session *Session) Value {
	session.Transaction = &Transaction{}
	return Value{Typ: "string", Str: "OK"}
}

func discardCommand(req *Value, session *Session) Value {
	if session.Transaction == nil {
		return Value{Typ: "error", Str: "ERR DISCARD without MULTI"}
	}
	session.Transaction = nil
	return Value{Typ: "string", Str: "OK"}
}

func sendCommandToReplicas(req *Value) {
	if config.Role == "slave" {
		return
	}
	sreq := Serialize(*req)
	GlobalInstanceOffset += len(sreq)
	OpenNotifications <- struct{}{}
	for _, replica := range GlobalReplicas {
		replica.c <- sreq
	}
}

func getCommand(req *Value) Value {
	key := req.Arr[1].Str
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

func typeCommand(req *Value) Value {
	key := req.Arr[1].Str
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

func configCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "CONFIG requires at least 2 arguments"}
	}
	if args[1].Str == "GET" {
		if args[2].Str == "dir" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dir"}, {Typ: "bstring", Str: config.Dir}}}
		}
		if args[2].Str == "dbfilename" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dbfilename"}, {Typ: "bstring", Str: config.DbFilename}}}
		}
	}
	return Value{Typ: "error", Str: "Invalid CONFIG command"}
}

func keysCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "KEYS requires at least 1 argument"}
	}
	if args[1].Str != "*" {
		return Value{Typ: "error", Str: "Invalid argument"}
	}
	keys := []Value{}
	for k := range GlobalMap {
		keys = append(keys, Value{Typ: "bstring", Str: k})
	}
	return Value{Typ: "array", Arr: keys}
}

func infoCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "INFO requires at least 1 argument"}
	}
	if args[1].Str == "replication" {
		return Value{Typ: "bstring", Str: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d\n", config.Role, master_replid, GlobalInstanceOffset)}
	}
	return Value{Typ: "error", Str: "Invalid INFO command"}
}
