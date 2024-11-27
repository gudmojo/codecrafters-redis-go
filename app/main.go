package main

import (
	"log"
	"os"
	"strconv"
)

var config Config

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
		go startReplication()
	}
	startServer()
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
			config.ReplicationMaster = os.Args[i+1]
			config.Role = "slave"
			if i+2 < len(os.Args) {
				port, err := strconv.Atoi(os.Args[i+2])
				if err != nil {
					log.Println("Error parsing master port:", err)
				}
				config.ReplicationPort = port
			}
		}
	}
	return config
}
