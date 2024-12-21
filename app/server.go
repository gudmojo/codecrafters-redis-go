package main

import (
	"bufio"
	"fmt"
	"io"
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
	Int         int
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	offset := 0
	defer conn.Close()
	reader := NewReader(bufio.NewReader(conn))
	for {
		_, req, err := reader.ParseArrayOfBstringValues()
		if err != nil {
			if err == io.EOF {
				Log("Client closed connection")
				return
			}
			Log(fmt.Sprintf("Error while parsing request: %v", err))
			res := Value{Typ: "error", Str: "Error parsing request"}
			conn.Write([]byte(Serialize(res)))
		} else if isAsyncRequestType(req) {
			HandleAsyncRequest(conn, reader, req)
		} else {
			res := HandleRequest(req, offset)
			offset = GlobalInstanceOffset
			ress := Serialize(res)
			Log(fmt.Sprintf("Writing response %s", ress))
			conn.Write([]byte(ress))
		}
	}
}

func isAsyncRequestType(req *Value) bool {
	return strings.ToUpper(req.Arr[0].Str) == "PSYNC"
}

func HandleAsyncRequest(conn net.Conn, reader *Reader, req *Value) {
	switch strings.ToUpper(req.Arr[0].Str) {
	case "PSYNC":
		psyncCommand(conn, reader, req)
	}
}

func HandleRequest(req *Value, offset int) Value {
	cmd := req.Arr[0].Str
	Log(fmt.Sprintf("HANDLE REQUEST: %s %s", cmd, Serialize(*req)))
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
	case "WAIT":
		return waitCommand(req, offset)
	}
	return Value{Typ: "error", Str: "Unknown command"}
}
