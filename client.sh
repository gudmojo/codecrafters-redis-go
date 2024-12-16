#!/bin/sh

redis-cli -h localhost -p 6379 SET foo 1
redis-cli -h localhost -p 6379 SET bar 2
redis-cli -h localhost -p 6379 SET baz 3
sleep 1
redis-cli -h localhost -p 6379 GET foo
redis-cli -h localhost -p 6379 GET bar
redis-cli -h localhost -p 6379 GET baz
sleep 1
redis-cli -h localhost -p 6380 GET foo
redis-cli -h localhost -p 6380 GET bar
redis-cli -h localhost -p 6380 GET baz
