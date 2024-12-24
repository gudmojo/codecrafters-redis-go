package main

import (
	"fmt"
	"io"
	"strconv"
)

var LeaderID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

// Acks from replicas after leader sent getAck
var AckNotifications = make(chan *Replica, 10000)

// Master keeps a list of replicas
var GlobalReplicas = make([]*Replica, 0)

// Leader's handler for responses from replicas
func handleReplicaResponses(reader *Reader, r *Replica) {
	for {
		_, cmd, err := reader.ParseArrayOfBstringValues()
		if err != nil {
			if err == io.EOF {
				Log("Replica closed the connection")
				return
			}
			Log(fmt.Sprintf("Error while parsing replica response: %v", err))
		}
		if len(cmd.Arr) >= 3 && cmd.Arr[0].Str == "REPLCONF" && cmd.Arr[1].Str == "ACK" {
			offset, err := strconv.Atoi(cmd.Arr[2].Str)
			if err != nil {
				Log(fmt.Sprintf("Error parsing offset: %v", err))
			}
			r.offset = offset
			AckNotifications <- r
		}
	}
}

func sendCommandToReplicas(req *Value) {
	if config.Role == "replica" {
		return
	}
	sreq := Serialize(*req)
	GlobalInstanceOffset += len(sreq)
	for _, replica := range GlobalReplicas {
		replica.c <- sreq
	}
}
