package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)

type MapValue struct {
	Typ    string
	Exp    time.Time
	Str    string
	Stream []StreamValue
	LastId StreamId
	Chans  []chan struct{}
}

type Value struct {
	Typ         string
	Str         string
	Arr         []Value
	PsyncHeader *Value
	PsyncData   *Value
	Bytes       []byte
}

func startServer() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	Log(fmt.Sprintf("Server started on port %d", config.Port))
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(&conn)
	}
}

func handleConnection(conn *net.Conn) {
	defer (*conn).Close()

	for {
		var res Value
		// Read incoming data
		requestBytes := make([]byte, 1024)
		Log(fmt.Sprintf("Master reading"))
		_, err := (*conn).Read(requestBytes)
		Log(fmt.Sprintf("Master read"))
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Printf("Client closed connection")
				return
			}
			fmt.Printf("Error in handleConnection read: %v", err)
			return
		}
		req, err := Parse(requestBytes)
		if err != nil {
			fmt.Println(err)
			res = Value{Typ: "error", Str: "Error parsing request"}
			(*conn).Write([]byte(Serialize(res)))
		} else if isAsyncRequestType(req) {
			HandleAsyncRequest(conn, req)
		} else {
			res = HandleRequest(req)
			Log(fmt.Sprintf("Master writing"))
			(*conn).Write([]byte(Serialize(res)))
			Log(fmt.Sprintf("Master wrote"))
		}
	}
}

func isAsyncRequestType(req *Value) bool {
	return strings.ToUpper(req.Arr[0].Str) == "PSYNC"
}

func HandleAsyncRequest(conn *net.Conn, req *Value) {
	switch strings.ToUpper(req.Arr[0].Str) {
	case "PSYNC":
		psyncCommand(conn, req)
	}
}

func HandleRequest(req *Value) Value {
	cmd := req.Arr[0].Str
	Log(fmt.Sprintf("HANDLE REQUEST: %s", cmd))
	switch strings.ToUpper(cmd) {
	case "PING":
		return pingCommand()
	case "ECHO":
		return echoCommand(req)
	case "SET":
		return setCommand(req)
	case "GET":
		return getCommand(req)
	case "TYPE":
		return typeCommand(req)
	case "XADD":
		return xadd(req)
	case "XRANGE":
		return xrange(req)
	case "XREAD":
		return xread(req)
	case "CONFIG":
		return configCommand(req)
	case "KEYS":
		return keysCommand(req)
	case "INFO":
		return infoCommand(req)
	case "REPLCONF":
		return replconfCommand(req)
	}
	return Value{Typ: "error", Str: "Unknown command"}
}
