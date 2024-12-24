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
	if config.Role == "replica" {
		Log("Starting as a replica")
		go startReplica()
		// wait forever - we want replica to keep running even if connection to leader was lost
		select {}
	} else {
		Log("Starting as a leader")
		startServer()
	}
}

func parseArgs() Config {
	config := Config{
		Port: 6379,
		Role: "leader",
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
			config.Role = "replica"
			s := strings.Split(os.Args[i+1], " ")
			config.ReplicationLeader = s[0]
			if i+1 < len(os.Args) {
				port, err := strconv.Atoi(s[1])
				if err != nil {
					Log(fmt.Sprintf("Error parsing leader port: %v", err))
				}
				config.ReplicationPort = port
			}
			Log(fmt.Sprintf("Replicating from %s:%d", config.ReplicationLeader, config.ReplicationPort))
		}
	}
	return config
}
