package main

import (
	"fmt"
	"strconv"
	"time"
)

func waitCommand(req *Value, offset int) Value {
	replicationFactor, err := strconv.Atoi(req.Arr[1].Str)
	if err != nil {
		Log(fmt.Sprintf("Error parsing replication factor: %v", err))
		return Value{Typ: "error", Str: "Error parsing replication factor"}
	}
	timeout, err := strconv.Atoi(req.Arr[2].Str)
	if err != nil {
		Log(fmt.Sprintf("Error parsing timeout: %v", err))
		return Value{Typ: "error", Str: "Error parsing timeout"}
	}
	done := map[int]bool{}
	for _, r := range GlobalReplicas {
		if r.offset >= offset {
			done[r.id] = true
			continue
		}
		go func() {
			r.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		}()
	}
	outer:
	for len(done) < replicationFactor {
		select {
		case c := <-AckNotifications:
			if c.offset >= offset {
				done[c.id] = true
				if len(done) >= replicationFactor {
					break
				}
			}
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			break outer
		}
	}
	return Value{Typ: "int", Int: len(done)}
}
