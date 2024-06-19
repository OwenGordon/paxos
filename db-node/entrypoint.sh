#!/bin/sh

redis-server --daemonize yes
sleep 5

./target/release/db-node
