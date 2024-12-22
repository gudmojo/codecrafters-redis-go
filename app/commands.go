package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

func pingCommand() Value {
	return Value{Typ: "string", Str: "PONG"}
}

func replconfCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "REPLCONF requires at least 2 arguments"}
	}
	switch strings.ToUpper(args[1].Str) {
	case "LISTENING-PORT":
		port, err := strconv.Atoi(args[2].Str)
		if err != nil {
			return Value{Typ: "error", Str: "Error parsing port"}
		}
		Log(fmt.Sprintf("Replica listening port: %d", port))
		// TODO: Save the port
	case "CAPA":
		Log(fmt.Sprintf("replsync capa: %s", args[2].Str))
	case "GETACK":
		return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "REPLCONF"}, {Typ: "bstring", Str: "ACK"}, {Typ: "bstring", Str: strconv.Itoa(GlobalInstanceOffset)}}}
	}
	return Value{Typ: "string", Str: "OK"}
}

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

func echoCommand(req *Value) Value {
	return Value{Typ: "string", Str: req.Arr[1].Str}
}

type Replica struct {
	id int
	c chan []byte
	offset int
	conn net.Conn
}

func NewReplica(id int, conn net.Conn) *Replica {
	return &Replica{id: id, conn: conn, c: make(chan []byte, 10000), offset: 0}
}

func setCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "SET requires at least 2 arguments"}
	}
	if len(args) == 3 {
		key := args[1].Str
		value := args[2].Str
		GlobalMap[key] = &MapValue{Typ: "string", Str: value}
		Log(fmt.Sprintf("SET key: %s, value: %s", key, value))
		sendCommandToReplicas(req)
		return Value{Typ: "string", Str: "OK"}
	}
	if len(args) == 5 {
		key := args[1].Str
		value := args[2].Str
		exp := args[4].Str
		currentTime := time.Now()
		ms, ok := strconv.Atoi(exp)
		if ok != nil {
			return Value{Typ: "error", Str: "Error parsing milliseconds"}
		}
		futureTime := currentTime.Add(time.Duration(ms) * time.Millisecond)
		GlobalMap[key] = &MapValue{Typ: "string", Str: value, Exp: futureTime}
		Log(fmt.Sprintf("SET key: %s, value: %s, exp: %s", key, value, futureTime))
		sendCommandToReplicas(req)
		return Value{Typ: "string", Str: "OK"}
	}
	return Value{Typ: "error", Str: "SET requires 2 or 4 arguments"}
}

func incrCommand(req *Value) Value {
	var i int
	var err error
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "INCR requires at least 1 argument"}
	}
	key := args[1].Str
	val, ok := GlobalMap[key]
	if !ok {
		i = 1
	} else {
		if val.Typ != "string" {
			return Value{Typ: "error", Str: "Value is not a string"}
		}
		i, err = strconv.Atoi(val.Str)
		if err != nil {
			return Value{Typ: "error", Str: "ERR value is not an integer or out of range"}
		}
		i++
	}
	GlobalMap[key] = &MapValue{Typ: "string", Str: strconv.Itoa(i)}
	sendCommandToReplicas(req)
	return Value{Typ: "int", Int: i}
}

func multiCommand(req *Value) Value {
	return Value{Typ: "string", Str: "OK"}
}

func sendCommandToReplicas(req *Value) {
	if config.Role == "slave" {
		return
	}
	Log(fmt.Sprintf("Sending command to replicas, %d", len(GlobalReplicas)))
	sreq := Serialize(*req)
	GlobalInstanceOffset += len(sreq)
	OpenNotifications <- struct{}{}
	for _, replica := range GlobalReplicas {
		Log("Sending command to replica")
		replica.c <- sreq
		Log("Sent command to replica")
	}
}

func getCommand(req *Value) Value {
	key := req.Arr[1].Str
	value, ok := GlobalMap[key]
	if !ok {
		return Value{Typ: "bstring", Str: ""}
	}
	if !value.Exp.IsZero() && value.Exp.Before(time.Now()) {
		delete(GlobalMap, key)
		return Value{Typ: "bstring", Str: ""}
	}
	return Value{Typ: "bstring", Str: value.Str}
}

func typeCommand(req *Value) Value {
	key := req.Arr[1].Str
	value, ok := GlobalMap[key]
	if !ok {
		return Value{Typ: "string", Str: "none"}
	}
	if !value.Exp.IsZero() && value.Exp.Before(time.Now()) {
		delete(GlobalMap, key)
		return Value{Typ: "string", Str: "none"}
	}
	return Value{Typ: "string", Str: value.Typ}
}

func configCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 3 {
		return Value{Typ: "error", Str: "CONFIG requires at least 2 arguments"}
	}
	if args[1].Str == "GET" {
		if args[2].Str == "dir" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dir"}, {Typ: "bstring", Str: config.Dir}}}
		}
		if args[2].Str == "dbfilename" {
			return Value{Typ: "array", Arr: []Value{{Typ: "bstring", Str: "dbfilename"}, {Typ: "bstring", Str: config.DbFilename}}}
		}
	}
	return Value{Typ: "error", Str: "Invalid CONFIG command"}
}

func keysCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "KEYS requires at least 1 argument"}
	}
	if args[1].Str != "*" {
		return Value{Typ: "error", Str: "Invalid argument"}
	}
	keys := []Value{}
	for k := range GlobalMap {
		keys = append(keys, Value{Typ: "bstring", Str: k})
	}
	return Value{Typ: "array", Arr: keys}
}

func infoCommand(req *Value) Value {
	args := req.Arr
	if len(args) < 2 {
		return Value{Typ: "error", Str: "INFO requires at least 1 argument"}
	}
	if args[1].Str == "replication" {
		return Value{Typ: "bstring", Str: fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d\n", config.Role, master_replid, master_repl_offset)}
	}
	return Value{Typ: "error", Str: "Invalid INFO command"}
}

// In order to make the wait command work, the master needs to know the offsets of all the replicas
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
