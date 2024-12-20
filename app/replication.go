package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

func startReplica() {
	conn := connectToMaster()
	defer (*conn).Close()
	ping(conn)
	replConf(conn, "listening-port", strconv.Itoa(config.Port))
	replConf(conn, "capa", "psync2")
	psync(conn, []string{"?", "-1"})

	// Start responding to read requests
	go startServer()

	// Listen to updates from master
	reader := NewReader(bufio.NewReader(*conn))
	for {
		Log("Replica waiting for update")
		for {
			var req *Value
			req, err := reader.ParseArrayOfBstringValues()
			if err != nil {
				if err == io.EOF {
					Log("Master closed the connection")
					return
				}
				Log(fmt.Sprintf("Error while parsing request: %v", err))
			} else {
				Log(fmt.Sprintf("Replica received command: %v", req))
				_ = HandleRequest(req)
			}	
		}
	}
}

func ping(conn *net.Conn) {
	_, err := (*conn).Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	b, err := (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response to ping:", string(reply[:b]))
}

func replConf(conn *net.Conn, key string, value string) {
	_, err := (*conn).Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: key}, {Typ: "bstring", Str: value}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	b, err := (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response to replconf:", string(reply[:b]))
}

func psync(conn *net.Conn, args []string) {
	a := make([]Value, 0, len(args)+1)
	a = append(a, Value{Typ: "bstring", Str: "PSYNC"})
	for _, arg := range args {
		a = append(a, Value{Typ: "bstring", Str: arg})
	}
	ser := Serialize(Value{Typ: "array", Arr: a})
	Log("Replica Sending PSYNC")
	_, err := (*conn).Write([]byte(ser))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 1024)
	b, err := (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Replica received response to psync:", string(reply[:b]))
}

func connectToMaster() *net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationMaster, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return &conn
}
