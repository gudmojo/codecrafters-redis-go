package main

import (
	"github.com/codecrafters-io/redis-starter-go/pkg/server"
	"github.com/codecrafters-io/redis-starter-go/pkg/rdb"
	"net"
	"os"
	"fmt"
)

func main() {
	parseArgs()
	rdb.LoadFile(server.ConfigDir, server.ConfigDbFilename)
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
		var res server.Value
		cmd, err := server.Parse(buf)
		if err != nil {
			fmt.Println(err)
			res = server.Value{Typ: "error", Str: "Error parsing command"}
		} else {
			res = server.HandleCommand(cmd)
		}
		serialzed := server.Serialize(res)
		conn.Write([]byte(serialzed))
	}
}

func parseArgs() {
	if len(os.Args) < 2 {
		return
	}
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--dir" {
			if i + 1 < len(os.Args) {
				server.ConfigDir = os.Args[i + 1]
			}
		}
		if os.Args[i] == "--dbfilename" {
			if i + 1 < len(os.Args) {
				// Load the data from the file
				server.ConfigDbFilename = os.Args[i + 1]
			}
		}
	}
}
