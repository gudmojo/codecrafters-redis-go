package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"
	"time"
)

var globalMap = make(map[string]MapValue)

func main() {
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
		// res := Value{typ: "error", str: "Error parsing command"}
		// Skip the first token
		// Skip the \r\n and $
		// Skip the \r\n
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

func parse(buf []byte) ([]Value, error) {
	log.Println("parse")
	i := 1 // Skip *
	log.Printf("i=%d", i)
	i, c, err := ReadNumber(buf, i)
	log.Printf("i=%d", i)
	log.Println("ii", i)
	if err != nil {
		log.Println("Error reading number of arguments")
		return nil, err
	}
	//log.Println("c", c)
	cmd := make([]Value, c)
	for j := 0; j < c; j++ {
		i += 3 // Skip \r\n$
		log.Printf("1 i=%d", i)
		var bulkLen int
		i, bulkLen, err = ReadNumber(buf, i)
		log.Printf("2 i=%d", i)
		//log.Println("bulkLen", bulkLen)
		if err != nil {
			log.Println("Y")
			log.Println("Error reading bulk length")
			return nil, err
		}
		i += 2 // Skip \r\n
		log.Printf("3 i=%d", i)
		value := string(buf[i : i+bulkLen])
		//log.Println("value", value)
		cmd[j] = Value{typ: "bstring", str: value}
		//log.Println("x")
		i += bulkLen
		log.Printf("4 i=%d", i)
	}
	return cmd, nil
}

func serialize(v Value) string {
	switch v.typ {
	case "bstring":
		if v.str == "" {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\n\r%s\r\n", len(v.str), v.str)
	case "string":
		return fmt.Sprintf("+%s\r\n", v.str)
	case "error":
		return fmt.Sprintf("-%s\r\n", v.str)
	}
	return ""
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
	}
	return Value{typ: "error", str: "Unknown command"}
}

func ReadNumber(s []byte, i int) (int, int, error) {
	log.Printf("ReadNumber %d", i)
	j := i
	for ; j < len(s) && unicode.IsDigit(rune(s[j])); {
		j++
	}
	x, err := strconv.ParseInt(string(s[i:j]), 10, 64)
	if err != nil {
		log.Println("Error parsing number:", err)
		return 0, 0, err
	}
	return j, int(x), nil
}

func ping() Value {
	return Value{typ: "string", str: "PONG"}
}

func echo(arg string) Value {
	return Value{typ: "string", str: arg}
}

type MapValue struct {
	value string
	exp time.Time
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "SET requires at least 2 arguments"}
	}
	if len(args) == 2 {
		globalMap[args[0].str] = MapValue{value: args[1].str}
		return Value{typ: "string", str: "OK"}
	}
	if len(args) == 4 {
		currentTime := time.Now() 
		ms, ok := strconv.ParseInt(args[3].str, 10, 64)
		if ok != nil {
			return Value{typ: "error", str: "Error parsing milliseconds"}
		}
		futureTime := currentTime.Add(time.Duration(ms) * time.Millisecond)
		globalMap[args[0].str] = MapValue{value: args[1].str, exp: futureTime}
		return Value{typ: "string", str: "OK"}
	}
	return Value{typ: "error", str: "SET requires 2 or 4 arguments"}
}

func get(key string) Value {
	value, ok := globalMap[key]
	if !ok {
		return Value{typ: "bstring", str: ""}
	}
	if value.exp.Before(time.Now()) {
		delete(globalMap, key)
		return Value{typ: "bstring", str: ""}
	}
	return Value{typ: "bstring", str: value.value}
}

type Value struct {
	typ string
	str string
}
