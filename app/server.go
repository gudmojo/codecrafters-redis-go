package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"
)

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
	i := 1
	i, jmax, err := ReadNumber(buf, i)
	if err != nil {
		log.Println("Error reading number of arguments")
		return nil, err
	}
	cmd := make([]Value, jmax)
	for j := 0; j < jmax; j++ {
		i += 3
		i, bulkLen, err := ReadNumber(buf, i)
		if err != nil {
			log.Println("Error reading bulk length")
			return nil, err
		}
		i += 2
		value := string(buf[i : i+bulkLen])
		cmd[j] = Value{typ: "bstring", str: value}
	}
	return cmd, nil
}

func serialize(v Value) string {
	switch v.typ {
	case "bstring":
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
	}
	return Value{typ: "error", str: "Unknown command"}
}

func ReadNumber(s []byte, i int) (int, int, error) {
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

type Value struct {
	typ string
	str string
}
