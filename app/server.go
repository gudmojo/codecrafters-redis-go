package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// Track offset for replication status
var GlobalInstanceOffset = 0

func startServer() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		Log(fmt.Sprintf("Failed to bind to port %d", config.Port))
		os.Exit(1)
	}
	Log(fmt.Sprintf("Server started on port %d", config.Port))
	for {
		conn, err := l.Accept()
		if err != nil {
			Log(fmt.Sprintf("Error accepting connection: %v", err))
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	offset := 0
	defer conn.Close()
	reader := NewReader(bufio.NewReader(conn))
	session := &Session{}
	for {
		_, req, err := reader.ParseArrayOfBstringValues()
		if err != nil {
			if err == io.EOF {
				return
			}
			Log(fmt.Sprintf("Error while parsing request: %v", err))
			res := Value{Typ: "error", Str: "Error parsing request"}
			conn.Write([]byte(res.Serialize()))
		} else if isAsyncRequestType(req) {
			HandleAsyncRequest(conn, reader, req)
		} else {
			res := HandleRequest(req, offset, session)
			offset = GlobalInstanceOffset
			ress := res.Serialize()
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

func HandleRequest(req *Value, offset int, session *Session) Value {
	cmd := req.Arr[0].Str
	switch strings.ToUpper(cmd) {
	case "PING":
		return pingCommand()
	case "ECHO":
		return echoCommand(req)
	case "SET":
		if session.Transaction != nil {
			session.Transaction.Commands = append(session.Transaction.Commands, req)
			return Value{Typ: "string", Str: "QUEUED"}
		}
		return setCommand(req)
	case "INCR":
		if session.Transaction != nil {
			session.Transaction.Commands = append(session.Transaction.Commands, req)
			return Value{Typ: "string", Str: "QUEUED"}
		}
		return incrCommand(req)
	case "MULTI":
		return multiCommand(req, session)
	case "DISCARD":
		return discardCommand(req, session)
	case "EXEC":
		return execCommand(req, session)
	case "GET":
		if session.Transaction != nil {
			session.Transaction.Commands = append(session.Transaction.Commands, req)
			return Value{Typ: "string", Str: "QUEUED"}
		}
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
