package main

import (
	"encoding/base64"
	"fmt"
	"net"
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
		PsyncHeader: &Value{Typ: "string", Str: fmt.Sprintf("FULLRESYNC %s 0", LeaderID)},
		PsyncData:   &Value{Typ: "bytes", Bytes: bytes},
	}
	conn.Write([]byte(Serialize(res)))
	r := NewReplica(len(GlobalReplicas), conn)
	GlobalReplicas = append(GlobalReplicas, r)
	go handleReplicaResponses(reader, r)
	// Now that we have sent the full resync, we can start sending updates
	// Read the command from a channel and send it to the replica
	for {
		cmd := <-r.c
		conn.Write(cmd)
	}
}
