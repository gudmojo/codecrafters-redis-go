package main

import (
	"fmt"
	"io"
	"strconv"
)

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
	OpenNotifications <- struct{}{}
	for _, replica := range GlobalReplicas {
		replica.c <- sreq
	}
}
