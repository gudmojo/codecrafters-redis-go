package main

import (
	"log"
	"os"
	"strconv"
)

var config Config

func main() {
	config = parseArgs()
	rdbLoadFile()
	startServer()
}

func parseArgs() Config {
	config := Config{
		Port: 6379,
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
				// Load the data from the file
				port, err := strconv.Atoi(os.Args[i+1])
				if err != nil {
					log.Println("Error parsing port:", err)
				}
				config.Port = port
			}
		}
	}
	return config
}
