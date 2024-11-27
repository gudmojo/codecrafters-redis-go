package main

import (
	"fmt"
	"log"
	"net"
)

func startReplication() {
	ping()
}

func ping() {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicationMaster, config.ReplicationPort))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(Serialize(Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "PING"}}})))
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