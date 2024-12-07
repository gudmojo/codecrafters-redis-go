package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
)

func startReplication() {
	conn := connectToMaster()
	defer conn.Close()
	ping(conn)
	replConf(conn, "listening-port", strconv.Itoa(config.Port))
	replConf(conn, "capa", "psync2")
}

func ping(conn net.Conn) {
	_, err := conn.Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	_, err = conn.Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response from server:", string(reply))
}

func replConf(conn net.Conn, key string, value string) {
	_, err := conn.Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: key}, {Typ: "bstring", Str: value}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	_, err = conn.Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response from server:", string(reply))
}

func connectToMaster() net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationMaster, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return conn
}