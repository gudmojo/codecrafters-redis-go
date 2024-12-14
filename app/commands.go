package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"time"
	"net"
)

func pingCommand() Value {
	return Value{Typ: "string", Str: "PONG"}
}

func replconfCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "REPLCONF requires at least 2 arguments"}
	}
	switch args[1].Str {
	case "listening-port":
		port, err := strconv.Atoi(args[2].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Error parsing port"}
		}
		log.Printf("Replica listening port: %d", port)
		// TODO: Save the port
	case "capa":
		log.Printf("replsync capa: %s", args[2].Str)
	}
	return Value{Typ: "string", Str: "OK"}
}

func psyncCommand(conn *net.Conn, req *Value) {
	log.Printf("ZZZZZZZZZXXXXXXXXX")
	args := req.Arr
	if len(args) < 3 {
		panic("PSYNC requires at least 2 arguments")
	}
	replId := args[1].Str
	offset := args[2].Str
	log.Printf("PSYNC: %s %s", replId, offset)
	bytes, _ := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	res := Value{
		Typ: "psync", 
		PsyncHeader: &Value{Typ: "string", Str: fmt.Sprintf("FULLRESYNC %s 0", master_replid)}, 
		PsyncData: &Value{Typ: "bytes", Bytes: bytes},
	}
	(*conn).Write([]byte(Serialize(res)))
	c := make(chan Value, 10000)
	GlobalReplicas = append(GlobalReplicas, c)
	// Now that we have sent the full resync, we can start sending updates
	// Read the command from a channel and send it to the replica
	for cmd := range c {
		(*conn).Write([]byte(Serialize(cmd)))
	}
}

func echoCommand(req *Value) Value {
	return Value{Typ: "string", Str: req.Arr[1].Str}
}

var GlobalReplicas = make([]chan Value, 0)

func setCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "SET requires at least 2 arguments"}
	}
	if len(args) == 3 {
		key := args[1].Str
		value := args[2].Str
		GlobalMap[key] = &MapValue{Typ: "string", Str: value}
		log.Printf("SET key: %s, value: %s", key, value)
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
		log.Printf("SET key: %s, value: %s, exp: %s", key, value, futureTime)
		sendCommandToReplicas(req)
		return Value{Typ: "string", Str: "OK"}
	}
	return Value{Typ: "error", Str: "SET requires 2 or 4 arguments"}
}

func sendCommandToReplicas(req *Value) {
	for _, replica := range GlobalReplicas {
		replica <- *req
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
		return Value{Typ: "bstring", Str: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", config.Role, master_replid, master_repl_offset)}
	}
	return Value{Typ: "error", Str: "Invalid INFO command"}
}
