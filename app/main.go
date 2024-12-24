package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	config = parseArgs()
	rdbLoadFile()
	if config.Role == "slave" {
		Log("Starting as a slave")
		go startReplica()
		// wait forever - we want replica to keep running even if connection to master was lost
		select {}
	} else {
		Log("Starting as a master")
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
					Log(fmt.Sprintf("Error parsing port: %v", err))
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
					Log(fmt.Sprintf("Error parsing master port: %v", err))
				}
				config.ReplicationPort = port
			}
			Log(fmt.Sprintf("Replicating from %s:%d", config.ReplicationMaster, config.ReplicationPort))
		}
	}
	return config
}
