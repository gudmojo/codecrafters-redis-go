#!/bin/sh

redis-cli -h localhost -p 6379 SET mykey "Hello world"
redis-cli -h localhost -p 6379 GET mykey
sleep 1
redis-cli -h localhost -p 6380 GET mykey
redis-cli -h localhost -p 6379 INFO replication
redis-cli -h localhost -p 6380 INFO replication
