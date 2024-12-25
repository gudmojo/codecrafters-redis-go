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
	conn := connectToLeader()
	defer conn.Close()
	reader := NewParser(bufio.NewReader(conn))
	ping(conn, reader)
	replConf(conn, reader, "listening-port", strconv.Itoa(config.Port))
	replConf(conn, reader, "capa", "psync2")
	psync(conn, reader, []string{"?", "-1"})
	GlobalInstanceOffset = 0
	session := &Session{}
	// Listen to updates from leader
	for {
		var req *Value
		n, req, err := reader.ParseArrayOfBstringValues()
		if err != nil {
			if err == io.EOF {
				Log("Leader closed the connection")
				return
			}
			Log(fmt.Sprintf("Error while parsing request: %v", err))
		} else {
			if len(req.Arr) >= 2 && strings.ToUpper(req.Arr[0].Str) == "REPLCONF" && strings.ToUpper(req.Arr[1].Str) == "GETACK" {
				res := HandleRequest(req, 0, session)
				conn.Write([]byte(res.Serialize()))
			} else {
				_ = HandleRequest(req, 0, session)
			}
		}
		GlobalInstanceOffset += n
	}
}

func ping(conn net.Conn, reader *Parser) {
	req := Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}}
	_, err := conn.Write([]byte(req.Serialize()))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	n, _, err := reader.LineString()
	if err != nil {
		log.Fatalf("Failed to read ping response: %v", err)
	}
	GlobalInstanceOffset += n
}

func replConf(conn net.Conn, reader *Parser, key string, value string) {
	req := Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: key}, {Typ: "bstring", Str: value}}}
	_, err := conn.Write([]byte(req.Serialize()))
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

func psync(conn net.Conn, reader *Parser, args []string) {
	a := make([]Value, 0, len(args)+1)
	a = append(a, Value{Typ: "bstring", Str: "PSYNC"})
	for _, arg := range args {
		a = append(a, Value{Typ: "bstring", Str: arg})
	}
	ser := Value{Typ: "array", Arr: a}.Serialize()
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

func connectToLeader() net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationLeader, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return conn
}
