package main

// Configuration of this server instance
var config Config

type Config struct {
	Dir string
	DbFilename string
	Port int
	Role string
	ReplicationLeader string
	ReplicationPort int
}
