package main

import (
	"fmt"
	"strconv"
	"strings"
)

func replconfCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "REPLCONF requires at least 2 arguments"}
	}
	switch strings.ToUpper(args[1].Str) {
	case "LISTENING-PORT":
		port, err := strconv.Atoi(args[2].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Error parsing port"}
		}
		Log(fmt.Sprintf("Replica listening port: %d", port))
	case "CAPA":
		Log(fmt.Sprintf("replsync capa: %s", args[2].Str))
	case "GETACK":
		return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: "ACK"}, {Typ: "bstring", Str: strconv.Itoa(GlobalInstanceOffset)}}}
	}
	return Value{Typ: "string", Str: "OK"}
}
