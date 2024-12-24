package main

var config Config

var master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

var AckNotifications = make(chan *Replica, 10000)

var OpenNotifications = make(chan struct{}, 10000)

var GlobalReplicas = make([]*Replica, 0)

var GlobalInstanceOffset = 0

// The in-memory datastore
var GlobalMap = make(map[string]*MapValue)
