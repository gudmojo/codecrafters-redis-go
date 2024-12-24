package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

func startReplica() {
	conn := connectToMaster()
	defer conn.Close()
	reader := NewReader(bufio.NewReader(conn))
	ping(conn, reader)
	replConf(conn, reader, "listening-port", strconv.Itoa(config.Port))
	replConf(conn, reader, "capa", "psync2")
	psync(conn, reader, []string{"?", "-1"})
	GlobalInstanceOffset = 0
	session := &Session{}
	// Listen to updates from master
	for {
		Log("Replica waiting for update")
		var req *Value
		n, req, err := reader.ParseArrayOfBstringValues()
		if err != nil {
			if err == io.EOF {
				Log("Master closed the connection")
				return
			}
			Log(fmt.Sprintf("Error while parsing request: %v", err))
		} else {
			Log(fmt.Sprintf("Replica received command: %v", req))
			if len(req.Arr) >= 2 && strings.ToUpper(req.Arr[0].Str) == "REPLCONF" && strings.ToUpper(req.Arr[1].Str) == "GETACK" {
				res := HandleRequest(req, 0, session)
				conn.Write([]byte(Serialize(res)))
			} else {
				_ = HandleRequest(req, 0, session)
			}
		}
		GlobalInstanceOffset += n
	}
}

func ping(conn net.Conn, reader *Reader) {
	_, err := conn.Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	n, reply, err := reader.LineString()
	if err != nil {
		log.Fatalf("Failed to read ping response: %v", err)
	}
	GlobalInstanceOffset += n
	fmt.Println("Response to ping:", reply)
}

func replConf(conn net.Conn, reader *Reader, key string, value string) {
	_, err := conn.Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: key}, {Typ: "bstring", Str: value}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	n, reply, err := reader.LineString()
	GlobalInstanceOffset += n
	if err != nil {
		log.Fatalf("Failed to read replconf response: %v", err)
	}
	fmt.Println("Response to replconf:", reply)
}

func psync(conn net.Conn, reader *Reader, args []string) {
	a := make([]Value, 0, len(args)+1)
	a = append(a, Value{Typ: "bstring", Str: "PSYNC"})
	for _, arg := range args {
		a = append(a, Value{Typ: "bstring", Str: arg})
	}
	ser := Serialize(Value{Typ: "array", Arr: a})
	Log("Replica Sending PSYNC")
	_, err := conn.Write([]byte(ser))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}
	// Start responding to read requests
	go startServer()

	n, reply, err := reader.LineString()
	if err != nil {
		log.Fatalf("Failed to read psync response: %v", err)
	}
	GlobalInstanceOffset += n
	fmt.Println("Replica received response to psync:", reply)
	_, err = reader.ReadRdb()
	if err != nil {
		log.Fatalf("Failed to read psync response 2: %v", err)
	}
}

func connectToMaster() net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationMaster, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return conn
}
