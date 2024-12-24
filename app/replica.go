package main

import (
	"net"
)
type Replica struct {
	id int
	c chan []byte
	offset int
	conn net.Conn
}

func NewReplica(id int, conn net.Conn) *Replica {
	return &Replica{id: id, conn: conn, c: make(chan []byte, 10000), offset: 0}
}

