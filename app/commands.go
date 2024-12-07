package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func pingCommand() Value {
	return Value{Typ: "string", Str: "PONG"}
}

func replconfCommand(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "REPLCONF requires at least 2 arguments"}
	}
	switch args[0].Str {
	case "listening-port":
		port, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Error parsing port"}
		}
		log.Printf("Replica listening port: %d", port)
		// TODO: Save the port
	case "capa":
		log.Printf("replsync capa: %s", args[1].Str)
	}
	return Value{Typ: "string", Str: "OK"}
}

func psyncCommand(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "PSYNC requires at least 2 arguments"}
	}
	replId := args[0].Str
	offset := args[1].Str
	log.Printf("PSYNC: %s %s", replId, offset)
	return Value{Typ: "string", Str: fmt.Sprintf("FULLRESYNC %s 0", replId)}
}

func echoCommand(arg string) Value {
	return Value{Typ: "string", Str: arg}
}

func setCommand(args []Value) Value {
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

func getCommand(key string) Value {
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

func typeCommand(key string) Value {
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

func configCommand(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "CONFIG requires at least 2 arguments"}
	}
	if args[0].Str == "GET" {
		if args[1].Str == "dir" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dir"}, {Typ: "bstring", Str: config.Dir}}}
		}
		if args[1].Str == "dbfilename" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dbfilename"}, {Typ: "bstring", Str: config.DbFilename}}}
		}
	}
	return Value{Typ: "error", Str: "Invalid CONFIG command"}
}

func keysCommand(args []Value) Value {
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

func infoCommand(args []Value) Value {
	if len(args) < 1 {
		return Value{Typ: "error", Str: "INFO requires at least 1 argument"}
	}
	if args[0].Str == "replication" {
		master_replid := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		master_repl_offset := 0
		return Value{Typ: "bstring", Str: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", config.Role, master_replid, master_repl_offset)}
	}
	return Value{Typ: "error", Str: "Invalid INFO command"}
}
