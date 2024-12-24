package main

type Config struct {
	Dir string
	DbFilename string
	Port int
	Role string
	ReplicationMaster string
	ReplicationPort int
}
