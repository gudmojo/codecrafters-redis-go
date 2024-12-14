package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
)

func startReplica() {
	log.Println("X")
	conn := connectToMaster()
	log.Println("X")
	defer (*conn).Close()
	log.Println("X")
	ping(conn)
	log.Println("X")
	replConf(conn, "listening-port", strconv.Itoa(config.Port))
	log.Println("X")
	replConf(conn, "capa", "psync2")
	log.Println("XX")
	psync(conn, []string{"?", "-1"})
	log.Println("Replica XXX")
	// Listen to updates from master
	for {
		requestBytes := make([]byte, 1024)
		_, err := (*conn).Read(requestBytes)
		if err != nil {
			log.Printf("Replica failed to read update: %v", err)
			return
		}
		fmt.Println("Received update from master:", string(requestBytes))
		req, err := Parse(requestBytes)
		if err != nil {
			fmt.Println(err)
		} else {
			log.Printf("Replica received command: %v", req)
			_ = HandleRequest(req)
		}
	}
}

func ping(conn *net.Conn) {
	_, err := (*conn).Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	_, err = (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response to ping:", string(reply))
}

func replConf(conn *net.Conn, key string, value string) {
	_, err := (*conn).Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: key}, {Typ: "bstring", Str: value}}})))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 256)
	_, err = (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Response to replconf:", string(reply))
}

func psync(conn *net.Conn, args []string) {
	a := make([]Value, 0, len(args) + 1)
	a = append(a, Value{Typ: "bstring", Str: "PSYNC"})
	for _, arg := range args {
		a = append(a, Value{Typ: "bstring", Str: arg})
	}
	ser := Serialize(Value{Typ: "array", Arr: a})
	log.Printf("Replica Sending PSYNC")
	_, err := (*conn).Write([]byte(ser))
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	reply := make([]byte, 1024)
	_, err = (*conn).Read(reply)
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Println("Replica received response to psync:", string(reply))
}

func connectToMaster() *net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationMaster, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	return &conn
}