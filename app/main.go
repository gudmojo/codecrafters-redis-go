package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

var config Config
var master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var master_repl_offset = 0
var AckNotifications = make(chan *Replica, 10000)
var OpenNotifications = make(chan struct{}, 10000)
var GlobalReplicas = make([]*Replica, 0)
var GlobalInstanceOffset = 0

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
