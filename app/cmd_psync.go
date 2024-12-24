package main

import (
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"io"
)

func psyncCommand(conn net.Conn, reader *Reader, req *Value) {
	args := req.Arr
	if len(args) < 3 {
		panic("PSYNC requires at least 2 arguments")
	}
	replId := args[1].Str
	offset := args[2].Str
	Log(fmt.Sprintf("PSYNC: %s %s", replId, offset))
	bytes, _ := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	res := Value{
		Typ:         "psync",
		PsyncHeader: &Value{Typ: "string", Str: fmt.Sprintf("FULLRESYNC %s 0", master_replid)},
		PsyncData:   &Value{Typ: "bytes", Bytes: bytes},
	}
	conn.Write([]byte(Serialize(res)))
	r := NewReplica(len(GlobalReplicas), conn)
	Log("Adding replica to GlobalReplicas")
	GlobalReplicas = append(GlobalReplicas, r)
	Log("Added replica to GlobalReplicas")
	go handleReplicaResponses(reader, r)
	// Now that we have sent the full resync, we can start sending updates
	// Read the command from a channel and send it to the replica
	for {
		cmd := <-r.c
		conn.Write(cmd)
	}
}

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
