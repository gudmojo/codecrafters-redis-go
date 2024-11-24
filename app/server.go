package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// The in-memory datastore
var globalMap = make(map[string]*MapValue)
var configDir = ""
var configDbFilename = ""

type MapValue struct {
	typ string
	exp time.Time
	str string
	stream []StreamValue
	lastId StreamId
	chans []chan struct{}
}
// --dir /tmp/redis-files --dbfilename dump.rdb
func main() {
	parseArgs()
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)	
	}
}

func parseArgs() {
	if len(os.Args) < 2 {
		return
	}
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--dir" {
			if i + 1 < len(os.Args) {
				configDir = os.Args[i + 1]
			}
		}
		if os.Args[i] == "--dbfilename" {
			if i + 1 < len(os.Args) {
				// Load the data from the file
				configDbFilename = os.Args[i + 1]
			}
		}
	}
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

	for {
		// Read incoming data
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		var res Value
		cmd, err := parse(buf)
		if err != nil {
			fmt.Println(err)
			res = Value{typ: "error", str: "Error parsing command"}
		} else {
			res = handleCommand(cmd)
		}
		serialzed := serialize(res)
		conn.Write([]byte(serialzed))
	}
}

func handleCommand(cmd []Value) Value {
	switch strings.ToUpper(cmd[0].str) {
	case "PING":
		return ping()
	case "ECHO":
		return echo(cmd[1].str)
	case "SET":
		return set(cmd[1:])
	case "GET":
		return get(cmd[1].str)
	case "TYPE":
		return type0(cmd[1].str)
	case "XADD":
		return xadd(cmd[1:])
	case "XRANGE":
		return xrange(cmd[1:])
	case "XREAD":
		return xread(cmd[1:])
	case "CONFIG":
		return config(cmd[1:])
	}
	return Value{typ: "error", str: "Unknown command"}
}

func ping() Value {
	return Value{typ: "string", str: "PONG"}
}

func echo(arg string) Value {
	return Value{typ: "string", str: arg}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "SET requires at least 2 arguments"}
	}
	if len(args) == 2 {
		key := args[0].str
		value := args[1].str
		globalMap[key] = &MapValue{typ: "string", str: value}
		log.Printf("SET key: %s, value: %s", key, value)
		return Value{typ: "string", str: "OK"}
	}
	if len(args) == 4 {
		key := args[0].str
		value := args[1].str
		exp := args[3].str
		currentTime := time.Now() 
		ms, ok := strconv.Atoi(exp)
		if ok != nil {
			return Value{typ: "error", str: "Error parsing milliseconds"}
		}
		futureTime := currentTime.Add(time.Duration(ms) * time.Millisecond)
		globalMap[key] = &MapValue{typ: "string", str: value, exp: futureTime}
		log.Printf("SET key: %s, value: %s, exp: %s", key, value, futureTime)
		return Value{typ: "string", str: "OK"}
	}
	return Value{typ: "error", str: "SET requires 2 or 4 arguments"}
}

func get(key string) Value {
	value, ok := globalMap[key]
	if !ok {
		return Value{typ: "bstring", str: ""}
	}
	if !value.exp.IsZero() && value.exp.Before(time.Now()) {
		delete(globalMap, key)
		return Value{typ: "bstring", str: ""}
	}
	return Value{typ: "bstring", str: value.str}
}

func type0(key string) Value {
	value, ok := globalMap[key]
	if !ok {
		return Value{typ: "string", str: "none"}
	}
	if !value.exp.IsZero() && value.exp.Before(time.Now()) {
		delete(globalMap, key)
		return Value{typ: "string", str: "none"}
	}
	return Value{typ: "string", str: value.typ}
}

func config(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "CONFIG requires at least 2 arguments"}
	}
	if args[0].str == "GET" {
		if args[1].str == "dir" {
			return Value{typ: "array", arr: []Value{{typ: "bstring", str: "dir"}, {typ: "bstring", str: configDir}}} 
		}
		if args[1].str == "dbfilename" {
			return Value{typ: "array", arr: []Value{{typ: "bstring", str: "dbfilename"}, {typ: "bstring", str: configDbFilename}}} 
		}
	}
	return Value{typ: "error", str: "Invalid CONFIG command"}
}