package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
)

func startReplica() {
	Log("X")
	conn := connectToMaster()
	Log("X")
	defer (*conn).Close()
	Log("X")
	ping(conn)
	Log("X")
	replConf(conn, "listening-port", strconv.Itoa(config.Port))
	Log("X")
	replConf(conn, "capa", "psync2")
	Log("XX")
	psync(conn, []string{"?", "-1"})
	Log("Replica XXX")
	// Listen to updates from master
	r := bufio.NewReader(*conn)
	for {
		Log("Replica waiting for update")
		requestBytes := make([]byte, 1024)
		b, err := r.Read(requestBytes)
		if err != nil {
			if err.Error() == "EOF" {
				Log("Connection to master was closed")
				select{} // Block forever so that the replica doesn't terminate
			}
			Log(fmt.Sprintf("Replica failed to read update: %v", err))
			Log(fmt.Sprintf("Bytes read: %d", b))
			Log(fmt.Sprintf("Bytes read: %s", requestBytes[:b]))
			select{} // Block forever so that the replica doesn't terminate
		}
		fmt.Println("Received update from master:", string(requestBytes))
		req, err := Parse(requestBytes)
		if err != nil {
			fmt.Println(err)
		} else {
			Log(fmt.Sprintf("Replica received command: %v", req))
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
	a := make([]Value, 0, len(args)+1)
	a = append(a, Value{Typ: "bstring", Str: "PSYNC"})
	for _, arg := range args {
		a = append(a, Value{Typ: "bstring", Str: arg})
	}
	ser := Serialize(Value{Typ: "array", Arr: a})
	Log(fmt.Sprintf("Replica Sending PSYNC"))
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
