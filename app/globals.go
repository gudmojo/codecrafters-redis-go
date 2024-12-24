package main

var LeaderID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

// Acks from replicas after leader sent getAck
var AckNotifications = make(chan *Replica, 10000)

var OpenNotifications = make(chan struct{}, 10000)

var GlobalReplicas = make([]*Replica, 0)

// Track offset for replication status
var GlobalInstanceOffset = 0

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)

var config Config

type Config struct {
	Dir string
	DbFilename string
	Port int
	Role string
	ReplicationLeader string
	ReplicationPort int
}
