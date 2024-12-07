package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)

type MapValue struct {
	Typ string
	Exp time.Time
	Str string
	Stream []StreamValue
	LastId StreamId
	Chans []chan struct{}
}

type Value struct {
	Typ string
	Str string
	Arr []Value
	PsyncHeader *Value
	PsyncData *Value
	Bytes []byte
}

func startServer() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	log.Println("Server started on port", config.Port)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)	
	}
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

	for {
		var res Value
		// Read incoming data
		requestBytes := make([]byte, 1024)
		_, err := conn.Read(requestBytes)
		if err != nil {
			fmt.Println(err)
			return
		}
		req, err := Parse(requestBytes)
		if err != nil {
			fmt.Println(err)
			res = Value{Typ: "error", Str: "Error parsing request"}
		} else {
			res = HandleRequest(req)
		}
		conn.Write([]byte(Serialize(res)))
	}
}

func HandleRequest(req []Value) Value {
	switch strings.ToUpper(req[0].Str) {
	case "PING":
		return pingCommand()
	case "ECHO":
		return echoCommand(req[1].Str)
	case "SET":
		return setCommand(req[1:])
	case "GET":
		return getCommand(req[1].Str)
	case "TYPE":
		return typeCommand(req[1].Str)
	case "XADD":
		return xadd(req[1:])
	case "XRANGE":
		return xrange(req[1:])
	case "XREAD":
		return xread(req[1:])
	case "CONFIG":
		return configCommand(req[1:])
	case "KEYS":
		return keysCommand(req[1:])
	case "INFO":
		return infoCommand(req[1:])
	case "REPLCONF":
		return replconfCommand(req[1:])
	case "PSYNC":
		return psyncCommand(req[1:])
	}
return Value{Typ: "error", Str: "Unknown command"}
}
