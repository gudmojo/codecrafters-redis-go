package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

var config Config
var master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var master_repl_offset = 0

type Config struct {
	Dir string
	DbFilename string
	Port int
	Role string
	ReplicationMaster string
	ReplicationPort int
}

func main() {
	config = parseArgs()
	rdbLoadFile()
	if config.Role == "slave" {
		log.Println("Starting as a replica")
		startReplica()
	} else {
		log.Println("Starting as a master")
		startServer()
	}
}

func parseArgs() Config {
	config := Config{
		Port: 6379,
		Role: "master",
	}
	if len(os.Args) < 2 {
		return config
	}
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--dir" {
			if i+1 < len(os.Args) {
				config.Dir = os.Args[i+1]
			}
		}
		if os.Args[i] == "--dbfilename" {
			if i+1 < len(os.Args) {
				// Load the data from the file
				config.DbFilename = os.Args[i+1]
			}
		}
		if os.Args[i] == "--port" {
			if i+1 < len(os.Args) {
				port, err := strconv.Atoi(os.Args[i+1])
				if err != nil {
					log.Println("Error parsing port:", err)
				}
				config.Port = port
			}
		}
		if os.Args[i] == "--replicaof" {
			config.Role = "slave"
			s := strings.Split(os.Args[i+1], " ")
			config.ReplicationMaster = s[0]
			if i+1 < len(os.Args) {
				port, err := strconv.Atoi(s[1])
				if err != nil {
					log.Println("Error parsing master port:", err)
				}
				config.ReplicationPort = port
			}
			log.Printf("Replicating from %s:%d", config.ReplicationMaster, config.ReplicationPort)
		}
	}
	return config
}
